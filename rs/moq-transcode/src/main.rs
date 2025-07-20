mod config;
mod transcode;

use anyhow::Context;
use config::Config;
use moq_lite::{OriginConsumer, OriginProducer};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
	let config = Config::load()?;
	let (publisher, mut subscriber) = connect(&config).await?;

	while let Some(update) = subscriber.next().await {
		let (path, broadcast) = match update {
			moq_lite::OriginUpdate {
				suffix,
				active: Some(broadcast),
			} => (suffix, broadcast),
			_ => continue,
		};

		let publisher = publisher.clone();
		tokio::spawn(async move {
			if let Err(err) = transcode::run(path, hang::BroadcastConsumer::new(broadcast), publisher).await {
				tracing::error!("transcode failed: {:?}", err);
			}
		});
	}

	Ok(())
}

async fn connect(config: &Config) -> anyhow::Result<(OriginProducer, OriginConsumer)> {
	let client = config.client.init()?;

	let publisher = OriginProducer::default();
	let subscriber = OriginProducer::default();

	// Connect to the relay, subscribing to the root and preparing to publish from our root.
	let session = moq_lite::Session::connect(
		client.connect(config.url.clone()).await?,
		Some(publisher.consume_all()),
		Some(subscriber.clone()),
	)
	.await
	.context("failed to connect to relay")?;

	tracing::info!("connected to relay: {}", session.webtransport.url());

	// Return the producer for publishing, and a consumer for the requested prefix.
	Ok((publisher, subscriber.consume_prefix(&config.prefix)))
}
