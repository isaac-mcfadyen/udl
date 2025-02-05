use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct StoredConfig {
	key: Option<String>,
	url: Option<String>,
}

pub struct Config {
	config: StoredConfig,
}
impl Config {
	pub async fn new() -> eyre::Result<Self> {
		let config_dir =
			dirs::config_dir().ok_or_else(|| eyre::eyre!("Could not find config directory"))?;
		let config_dir = config_dir.join("udl");
		tokio::fs::create_dir_all(&config_dir).await?;

		let config_file = config_dir.join("config.json");
		match tokio::fs::read(&config_file).await {
			Ok(data) => {
				let config: StoredConfig = serde_json::from_slice(&data)?;
				Ok(Self { config })
			}
			Err(_) => {
				let config = StoredConfig {
					key: None,
					url: None,
				};
				let data = serde_json::to_vec(&config)?;
				tokio::fs::write(&config_file, &data).await?;
				Ok(Self { config })
			}
		}
	}
	pub fn key(&self) -> Option<String> {
		self.config.key.to_owned()
	}
	pub async fn set_key(&mut self, key: String) -> eyre::Result<()> {
		self.config.key = Some(key);
		self.save().await
	}
	pub fn url(&self) -> Option<String> {
		self.config.url.to_owned()
	}
	pub async fn set_url(&mut self, url: String) -> eyre::Result<()> {
		self.config.url = Some(url);
		self.save().await
	}

	async fn save(&self) -> eyre::Result<()> {
		let config_dir =
			dirs::config_dir().ok_or_else(|| eyre::eyre!("Could not find config directory"))?;
		let config_dir = config_dir.join("udl");
		tokio::fs::create_dir_all(&config_dir).await?;

		let config_file = config_dir.join("config.json");
		let data = serde_json::to_vec(&self.config)?;
		tokio::fs::write(&config_file, &data).await?;
		Ok(())
	}
}
