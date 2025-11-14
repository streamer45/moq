use anyhow::Result;
use bytes::Bytes;
use moq_lite::{Broadcast, Track};
use std::time::Duration;
use tokio::time::sleep;
use tracing::info;

pub async fn run() -> Result<()> {
	// Setup logging
	moq_native::Log {
		level: tracing::Level::DEBUG,
	}
	.init();

	info!("===== PUBLISHER: Starting =====");
	info!("Publishing groups rapidly to relay");

	// Create origin for publishing
	let origin = moq_lite::Origin::produce();

	// Run broadcast and session in parallel
	tokio::select! {
		res = publish_broadcast(origin.producer) => res,
		res = connect_session(origin.consumer) => res,
	}
}

async fn connect_session(origin: moq_lite::OriginConsumer) -> Result<()> {
	// Connect to relay with TLS verification disabled for local testing
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	let client = moq_native::Client::new(config)?;

	let url = url::Url::parse("https://localhost:4443/anon/bug-test")?;
	info!("PUBLISHER: Connecting to relay at {}", url);

	let connection = client.connect(url).await?;
	let session = moq_lite::Session::connect(connection, origin, None).await?;

	info!("PUBLISHER: Connected!");

	// Wait until session closes
	session.closed().await.map_err(Into::into)
}

async fn publish_broadcast(origin: moq_lite::OriginProducer) -> Result<()> {
	// Give the session time to connect
	sleep(Duration::from_millis(100)).await;

	let mut broadcast = Broadcast::produce();

	let mut track = broadcast.producer.create_track(Track {
		name: "fast-track".to_string(),
		priority: 0,
	});

	origin.publish_broadcast("", broadcast.consumer);

	info!("PUBLISHER: Starting to publish groups rapidly...");

	let mut published_count = 0;

	loop {
		let mut group = track.append_group();

		for _frame_num in 0..100 {
			let data = Bytes::from(vec![0u8; 500]);
			group.write_frame(data);
		}

		group.close();
		published_count += 1;

		if published_count % 50 == 0 {
			info!("PUBLISHER: Published {} groups", published_count);
		}

		// Crazy fast producer
		sleep(Duration::from_millis(2)).await;

		if published_count >= 1000 {
			info!("PUBLISHER: Finished publishing {} groups", published_count);
			break;
		}
	}

	info!("PUBLISHER: Finished publishing all groups");

	Ok(())
}
