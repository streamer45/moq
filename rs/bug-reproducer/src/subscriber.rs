use anyhow::{Context, Result};
use moq_lite::Origin;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info};

pub async fn run() -> Result<()> {
	// Setup logging
	moq_native::Log {
		level: tracing::Level::DEBUG,
	}
	.init();

	info!("===== SUBSCRIBER: Starting =====");

	// Give publisher time to connect and start publishing
	sleep(Duration::from_millis(500)).await;

	// Create origin for subscribing
	let origin = Origin::produce();

	// Connect to relay
	let mut config = moq_native::ClientConfig::default();
	config.tls.disable_verify = Some(true);
	let client = moq_native::Client::new(config)?;

	let url = url::Url::parse("https://localhost:4443/anon/bug-test")?;
	info!("SUBSCRIBER: Connecting to relay at {}", url);

	let connection = client.connect(url).await?;

	// Subscribe to the remote broadcasts
	let session = moq_lite::Session::connect(connection, None, origin.producer).await?;
	info!("SUBSCRIBER: Connected!");

	// Wait a moment for announcements
	sleep(Duration::from_millis(100)).await;

	// Subscribe to the broadcast and track
	let broadcast = origin.consumer.consume_broadcast("").context("broadcast not found")?;

	let mut track = broadcast.subscribe_track(&moq_lite::Track {
		name: "fast-track".to_string(),
		priority: 0,
	});

	info!("SUBSCRIBER: Subscribed to track, receiving groups...");

	let mut received_count = 0;

	loop {
		tokio::select! {
			group = track.next_group() => {
				match group {
					Ok(Some(mut group)) => {
						let sequence = group.info.sequence;

						// Read all frames from this group
						loop {
							match group.read_frame().await {
								Ok(Some(_frame)) => {
									received_count += 1;
								}
								Ok(None) => {
									// End of group
									info!("SUBSCRIBER: end of group");
									break;
								}
								Err(e) => {
									// Frame read error - just skip this group
									info!("SUBSCRIBER: Frame read error in group {}: {} (continuing...)", sequence, e);
									break;
								}
							}
						}

						if received_count % 50 == 0 {
							info!("SUBSCRIBER: Received {} frames (group {})", received_count, sequence);
						}
					}
					Ok(None) => {
						info!("SUBSCRIBER: Track ended normally");
						break;
					}
					Err(e) => {
						error!("SUBSCRIBER: Track error (skipping and continuing): {}", e);
						break;
					}
				}
			}

			closed = session.closed() => {
				closed?;
				info!("SUBSCRIBER: Session closed");
				break;
			}
		}

		if received_count >= 100000 {
			info!("SUBSCRIBER: Received {} frames without disconnection", received_count);
			break;
		}
	}

	Ok(())
}
