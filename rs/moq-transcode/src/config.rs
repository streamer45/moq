use clap::Parser;
use serde::{Deserialize, Serialize};
use url::Url;

#[derive(Parser, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
	/// The QUIC/TLS configuration for the client.
	#[cfg(feature = "native")]
	#[command(flatten)]
	#[serde(default)]
	pub client: moq_native::ClientConfig,

	/// Log configuration.
	#[cfg(feature = "native")]
	#[command(flatten)]
	#[serde(default)]
	pub log: moq_native::Log,

	/// The URL of the relay to connect to.
	#[arg(long, env = "RELAY_URL")]
	pub url: Url,

	/// The prefix to subscribe to.
	#[arg(long, default_value = "")]
	pub prefix: moq_lite::Path,

	/// If provided, load the configuration from this file.
	#[arg(long)]
	#[serde(default)]
	pub config: Option<String>,
}

impl Config {
	pub fn load() -> anyhow::Result<Self> {
		// A simple config loader.
		// TODO: Layering config from file, env, and args.
		let config = Config::parse();

		#[cfg(feature = "native")]
		config.log.init();
		tracing::trace!(?config, "final config");

		Ok(config)
	}
}
