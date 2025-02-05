use std::{
	fmt::Write,
	io::SeekFrom,
	path::PathBuf,
	sync::{atomic::AtomicI32, Arc},
};

use clap::Parser;
use eyre::bail;
use futures::{stream::FuturesUnordered, StreamExt, TryStreamExt};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use tokio::{
	fs::File,
	io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

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
	url: String,

	/// The authentication key.
	#[clap(long)]
	key: String,
}

#[derive(Debug, Parser)]
enum Subcommand {
	/// Upload a file.
	Upload(UploadArgs),

	/// Download a file.
	Download(DownloadArgs),
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

async fn upload(global: GlobalArgs, args: UploadArgs) -> eyre::Result<()> {
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
	let stats_url = format!("{}/stats", global.url.trim_end_matches('/'));
	let stats = client
		.get(&stats_url)
		.query(&[("key", &args.name)])
		.header("Authorization", &global.key)
		.send()
		.await?;
	if !args.force && stats.status() != StatusCode::NOT_FOUND {
		bail!(
			"A file with the name {:?} already exists, not overwriting without --force",
			args.name
		);
	}

	// Start multipart upload.
	let start_url = format!("{}/start-upload", global.url.trim_end_matches('/'));
	let mpu = client
		.post(&start_url)
		.query(&[("key", &args.name)])
		.header("Authorization", &global.key)
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

	let name = Arc::new(args.name.clone());
	let upload_id = Arc::new(mpu.upload_id.clone());
	let key = Arc::new(global.key.clone());

	let futures = FuturesUnordered::new();
	let ranges = split_ranges(
		meta.len(), // 50MB chunks.
		50 * 1024 * 1024,
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
		let url = format!("{}/upload-part", global.url.trim_end_matches('/'));
		futures.push(tokio::task::spawn(async move {
			let res = client
				.post(&url)
				.query(&[
					("key", name.as_str()),
					("uploadId", upload_id.as_str()),
					("partNumber", (i + 1).to_string().as_str()),
				])
				.header("Authorization", key.as_str())
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

	let parts = futures.collect::<Vec<_>>().await;
	let parts = parts.into_iter().flatten().collect::<Result<Vec<_>, _>>()?;

	// Complete multipart upload.
	let url = format!("{}/complete-upload", global.url.trim_end_matches('/'));
	let res = client
		.post(&url)
		.query(&[("key", &args.name), ("uploadId", &mpu.upload_id)])
		.header("Authorization", &global.key)
		.json(&parts)
		.send()
		.await?
		.error_for_status()?;
	let _ = res.json::<serde_json::Value>().await?;

	tracing::info!("Upload complete");

	Ok(())
}
async fn download(global: GlobalArgs, args: DownloadArgs) -> eyre::Result<()> {
	// If the file already exists, we don't want to overwrite it.
	if !args.force && tokio::fs::metadata(&args.path).await.is_ok() {
		bail!(
			"A file at {:?} already exists, not overwriting without --force",
			args.path
		);
	}

	// Open the file.
	let mut file = File::create(&args.path).await?;

	let url = format!("{}/download", global.url.trim_end_matches('/'));
	let client = reqwest::Client::new();
	let res = client
		.get(&url)
		.query(&[("key", &args.name)])
		.header("Authorization", &global.key)
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

#[tokio::main]
async fn main() {
	tracing_subscriber::fmt::fmt()
		.without_time()
		.with_file(false)
		.with_target(false)
		.init();

	const BOLD_WHITE: &str = "\x1b[1;37m";
	const CLEAR_COLOR: &str = "\x1b[0m";
	tracing::info!(
		"{}Starting UDL {}{}",
		BOLD_WHITE,
		std::env!("CARGO_PKG_VERSION"),
		CLEAR_COLOR
	);
	let args = Args::parse();
	let res = match args.subcommand {
		Subcommand::Upload(v) => upload(args.global, v).await,
		Subcommand::Download(v) => download(args.global, v).await,
	};
	match res {
		Ok(_) => {}
		Err(e) => {
			tracing::error!("{}", e);
			std::process::exit(1);
		}
	}
}
