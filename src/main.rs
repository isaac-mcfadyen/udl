mod config;

use std::{
	fmt::Write,
	io::SeekFrom,
	path::PathBuf,
	sync::{atomic::AtomicI32, Arc},
};

use clap::Parser;
use config::Config;
use eyre::bail;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::{
	fs::File,
	io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

const BOLD_WHITE: &str = "\x1b[1;37m";
const CLEAR_COLOR: &str = "\x1b[0m";

#[derive(Debug, Parser)]
struct Args {
	#[clap(flatten)]
	global: GlobalArgs,

	#[clap(subcommand)]
	subcommand: Subcommand,
}

#[derive(Debug, Parser)]
struct GlobalArgs {
	/// The URL of the worker.
	#[clap(long)]
	url: Option<String>,

	/// The authentication key.
	#[clap(long)]
	key: Option<String>,
}

#[derive(Debug, Parser)]
enum Subcommand {
	/// Upload a file.
	Upload(UploadArgs),

	/// Download a file.
	Download(DownloadArgs),

	/// Delete a file.
	Delete(DeleteArgs),

	/// List files, optionally filtering by a prefix.
	List(ListArgs),

	/// Save configuration to avoid having to pass it in every time.
	SaveConfig,
}

#[derive(Debug, Parser)]
struct UploadArgs {
	/// The name of the file.
	name: String,
	/// The path to the file to upload.
	path: PathBuf,
	/// Whether to overwrite the remote file if it already exists.
	#[clap(long)]
	force: bool,
}

#[derive(Debug, Parser)]
struct DownloadArgs {
	/// The name of the file.
	name: String,
	/// The path to save the file to.
	path: PathBuf,
	/// Whether to overwrite the file if it already exists.
	#[clap(long)]
	force: bool,
}

#[derive(Debug, Parser)]
struct DeleteArgs {
	/// The name of the file to delete.
	name: String,
}

#[derive(Debug, Parser)]
struct ListArgs {
	/// Optional prefix to filter by.
	prefix: Option<String>,
}

/// Splits a number into ranges, each with a maximum size of `chunk_size`.
/// The final chunk may be smaller than `chunk_size`.
fn split_ranges(num: u64, chunk_size: u64) -> Vec<(u64, u64)> {
	let mut ranges = Vec::new();
	let mut start = 0;
	while start < num {
		let end = (start + chunk_size).min(num);
		ranges.push((start, end));
		start = end;
	}
	ranges
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartUploadResponse {
	upload_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct UploadPartResponse {
	part_number: u64,
	etag: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
	key: String,
	size: u64,
	etag: String,
}

async fn upload(base_url: String, key: String, args: UploadArgs) -> eyre::Result<()> {
	// Make sure the target file exists.
	let meta = match tokio::fs::metadata(&args.path).await {
		Ok(v) => v,
		Err(_) => {
			eyre::bail!("The file at {:?} does not exist", args.path);
		}
	};

	// Make sure it's a file not a folder.
	if !meta.is_file() {
		eyre::bail!("The item at {:?} is not a file", args.path);
	}

	let client = reqwest::Client::new();

	// Check to see if this key already exists.
	let key_encoded = urlencoding::encode(&args.name);
	let url = format!(
		"{}/objects/{}/stats",
		base_url.trim_end_matches('/'),
		key_encoded
	);
	let stats = client
		.get(&url)
		.query(&[("key", &args.name)])
		.header("Authorization", &key)
		.send()
		.await?;
	if !args.force && stats.status() != StatusCode::NOT_FOUND {
		bail!(
			"A file with the name {:?} already exists, not overwriting without --force",
			args.name
		);
	}

	// Start multipart upload.
	let url = format!("{}/uploads/create", base_url.trim_end_matches('/'));
	let mpu = client
		.post(&url)
		.query(&[("key", &args.name)])
		.header("Authorization", &key)
		.send()
		.await?
		.error_for_status()?
		.json::<StartUploadResponse>()
		.await?;
	tracing::info!("Starting upload of {:?}...", args.path);

	let total_size = meta.len();
	let progress = Arc::new(AtomicI32::new(0));

	// Create a progress bar.
	let pb = ProgressBar::new(total_size);
	pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
			.unwrap()
			.with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
			.progress_chars("#>-"));
	pb.tick();

	let name = Arc::new(args.name.clone());
	let upload_id = Arc::new(mpu.upload_id.clone());
	let key = Arc::new(key.clone());

	let mut futures = Vec::new();
	let ranges = split_ranges(
		meta.len(), // 10MB chunks.
		10 * 1024 * 1024,
	);
	for (i, range) in ranges.into_iter().enumerate() {
		// Open and seek file.
		let mut f = File::open(&args.path).await?;
		f.seek(SeekFrom::Start(range.0)).await?;
		let f = f.take(range.1 - range.0);

		let name = name.clone();
		let upload_id = upload_id.clone();
		let key = key.clone();
		let progress = progress.clone();
		let pb = pb.clone();

		let client = client.clone();
		let url = format!("{}/uploads/upload-part", base_url.trim_end_matches('/'));
		futures.push(tokio::task::spawn(async move {
			let res = client
				.post(&url)
				.query(&[
					("key", name.as_str()),
					("uploadId", upload_id.as_str()),
					("partNumber", (i + 1).to_string().as_str()),
				])
				.header("Authorization", key.as_str())
				.header("Content-Length", (range.1 - range.0).to_string().as_str())
				.body(reqwest::Body::wrap_stream(
					tokio_util::io::ReaderStream::new(f),
				))
				.send()
				.await;
			let Ok(res) = res else {
				bail!("Failed to upload part");
			};
			let res = res.error_for_status()?;
			let part = match res.json::<UploadPartResponse>().await {
				Ok(v) => v,
				Err(e) => {
					bail!("Failed to parse response: {:?}", e);
				}
			};

			progress.fetch_add(
				(range.1 - range.0) as i32,
				std::sync::atomic::Ordering::Relaxed,
			);
			pb.set_position(progress.load(std::sync::atomic::Ordering::Relaxed) as u64);
			Ok(part)
		}));
	}

	// Run 5 uploads at once.
	let parts = futures::stream::iter(futures)
		.buffer_unordered(5)
		.collect::<Vec<_>>()
		.await;
	let parts = parts.into_iter().flatten().collect::<Result<Vec<_>, _>>()?;

	// Complete multipart upload.
	let url = format!("{}/uploads/complete", base_url.trim_end_matches('/'));
	let res = client
		.post(&url)
		.query(&[("key", &args.name), ("uploadId", &mpu.upload_id)])
		.header("Authorization", key.as_str())
		.json(&parts)
		.send()
		.await?
		.error_for_status()?;
	let _ = res.json::<serde_json::Value>().await?;

	tracing::info!("Upload complete");

	Ok(())
}
async fn download(base_url: String, key: String, args: DownloadArgs) -> eyre::Result<()> {
	// If the file already exists, we don't want to overwrite it.
	if !args.force && tokio::fs::metadata(&args.path).await.is_ok() {
		bail!(
			"A file at {:?} already exists, not overwriting without --force",
			args.path
		);
	}

	// Open the file.
	let mut file = File::create(&args.path).await?;

	let key_encoded = urlencoding::encode(&args.name);
	let url = format!(
		"{}/objects/{}/download",
		base_url.trim_end_matches('/'),
		key_encoded
	);
	let client = reqwest::Client::new();
	let res = client
		.get(&url)
		.query(&[("key", &args.name)])
		.header("Authorization", &key)
		.send()
		.await?
		.error_for_status()?;

	let total_size = res.content_length().unwrap_or(0);
	let mut progress = 0;

	// Create a progress bar.
	let pb = ProgressBar::new(total_size);
	pb.set_style(ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
	pb.tick();

	// Download the file.
	let mut stream = res
		.bytes_stream()
		.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
	while let Some(chunk) = stream.try_next().await? {
		file.write_all(&chunk).await?;
		progress += chunk.len() as u64;
		pb.set_position(progress);
	}

	// Finish the progress bar.
	pb.finish_with_message("Download complete");

	Ok(())
}
async fn delete(base_url: String, key: String, args: DeleteArgs) -> eyre::Result<()> {
	let key_encoded = urlencoding::encode(&args.name);
	let url = format!(
		"{}/objects/{}/stats",
		base_url.trim_end_matches('/'),
		key_encoded
	);
	let client = reqwest::Client::new();

	// Make sure the file exists.
	let res = client
		.get(&url)
		.header("Authorization", &key)
		.send()
		.await?;
	if res.status() == StatusCode::NOT_FOUND {
		bail!("The file {:?} does not exist", args.name);
	}

	// Delete the file.
	let url = format!("{}/objects/{}", base_url.trim_end_matches('/'), key_encoded);
	client
		.delete(&url)
		.header("Authorization", &key)
		.send()
		.await?
		.error_for_status()?;
	tracing::info!("Deleted {:?}", args.name);
	Ok(())
}
async fn list(base_url: String, key: String, args: ListArgs) -> eyre::Result<()> {
	let client = reqwest::Client::new();
	let url = format!("{}/objects", base_url.trim_end_matches('/'));
	let res = client
		.get(&url)
		.query(&[("prefix", args.prefix.as_deref())])
		.header("Authorization", &key)
		.send()
		.await?
		.error_for_status()?;
	let res = res.json::<Vec<ListResponse>>().await?;
	if res.is_empty() {
		println!("{}No files found{}", BOLD_WHITE, CLEAR_COLOR);
		return Ok(());
	}

	println!(
		"{}{:<20} {:>10}{}",
		BOLD_WHITE, "Key", "Size (KB)", CLEAR_COLOR
	);
	for item in res {
		println!("{:<20} {:>10}", item.key, item.size / 1024);
	}
	Ok(())
}

#[tokio::main]
async fn main() {
	tracing_subscriber::fmt::fmt()
		.without_time()
		.with_file(false)
		.with_target(false)
		.init();

	tracing::info!(
		"{}Starting UDL {}{}",
		BOLD_WHITE,
		std::env!("CARGO_PKG_VERSION"),
		CLEAR_COLOR
	);

	let Ok(mut config) = Config::new().await else {
		tracing::error!("Failed to load config");
		std::process::exit(1);
	};
	let args = Args::parse();

	// Handle save config early.
	if matches!(args.subcommand, Subcommand::SaveConfig) {
		let (Some(url), Some(key)) = (args.global.url, args.global.key) else {
			tracing::error!("Both --url and --key must be provided to save config");
			std::process::exit(1);
		};
		if config.set_key(key).await.is_err() {
			tracing::error!("Failed to save key");
			std::process::exit(1);
		}
		if config.set_url(url).await.is_err() {
			tracing::error!("Failed to save url");
			std::process::exit(1);
		}
		tracing::info!("Saved config");
		tracing::warn!("Note that credentials are saved in clear-text!");
		std::process::exit(0);
	}

	let key = args.global.key.or(config.key());
	let url = args.global.url.or(config.url());
	let Some(key) = key else {
		tracing::error!("No key saved or provided as --key flag");
		std::process::exit(1);
	};
	let Some(url) = url else {
		tracing::error!("No url saved or provided as --url flag");
		std::process::exit(1);
	};

	let res = match args.subcommand {
		Subcommand::Upload(v) => upload(url, key, v).await,
		Subcommand::Download(v) => download(url, key, v).await,
		Subcommand::Delete(v) => delete(url, key, v).await,
		Subcommand::List(v) => list(url, key, v).await,
		_ => unreachable!("Checked above"),
	};
	match res {
		Ok(_) => {}
		Err(e) => {
			tracing::error!("{}", e);
			std::process::exit(1);
		}
	}
}
