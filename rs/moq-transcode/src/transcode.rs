use anyhow::{anyhow, Result};
use hang::{
	catalog::{Audio, AudioCodec, AudioConfig},
	BroadcastConsumer, BroadcastProducer, TrackConsumer, TrackProducer,
};
use moq_lite::{OriginProducer, Path};
use std::collections::HashSet;

pub async fn run(path: Path, broadcast: BroadcastConsumer, publisher: OriginProducer) -> Result<()> {
	let mut merged_broadcast = BroadcastProducer::new();
	publisher.publish(path.clone(), merged_broadcast.consume());
	tracing::info!("publishing merged broadcast: {}", path);

	let mut catalog_consumer = broadcast.catalog.clone();
	let mut processed_tracks = HashSet::new();

	while let Some(catalog) = catalog_consumer.next().await? {
		for video in &catalog.video {
			if processed_tracks.insert(video.track.name.clone()) {
				let consumer = broadcast.subscribe(&video.track);
				let producer = merged_broadcast.create_video(video.clone());
				tokio::spawn(proxy_track(consumer, producer));
			}
		}

		for audio in &catalog.audio {
			if processed_tracks.insert(audio.track.name.clone()) {
				let consumer = broadcast.subscribe(&audio.track);
				let producer = merged_broadcast.create_audio(audio.clone());
				tokio::spawn(proxy_track(consumer.clone(), producer));

				if let AudioCodec::Opus = &audio.config.codec {
					let mut transcoded_info = audio.clone();
					transcoded_info.track.name = format!("{}-transcoded", audio.track.name);
					transcoded_info.config.bitrate = Some(32_000);

					let transcoded_producer = merged_broadcast.create_audio(transcoded_info);
					let config = audio.config.clone();

					tokio::spawn(async move {
						if let Err(e) = transcode_track(consumer, transcoded_producer, config).await {
							tracing::error!("failed to transcode track: {}", e);
						}
					});
				}
			}
		}
	}

	Ok(())
}

async fn proxy_track(mut input: TrackConsumer, mut output: TrackProducer) {
	while let Ok(Some(frame)) = input.read().await {
		output.write(frame);
	}
}

async fn transcode_track(mut input: TrackConsumer, mut output: TrackProducer, config: AudioConfig) -> Result<()> {
	let channels = match config.channel_count {
		1 => opus::Channels::Mono,
		2 => opus::Channels::Stereo,
		_ => return Err(anyhow!("unsupported channel count: {}", config.channel_count)),
	};

	let mut decoder = opus::Decoder::new(config.sample_rate, channels)?;
	let mut encoder = opus::Encoder::new(config.sample_rate, channels, opus::Application::Audio)?;

	if let Some(bitrate) = config.bitrate {
		encoder.set_bitrate(opus::Bitrate::Bits(bitrate as i32))?;
	}

	let mut decoded_buffer = vec![0i16; 1920]; // Max opus frame size
	let mut encoded_buffer = vec![0u8; 4000]; // opus max packet size

	while let Some(frame) = input.read().await? {
		let samples = decoder.decode(&frame.payload, &mut decoded_buffer, false)?;
		let size = encoder.encode(&decoded_buffer[..samples], &mut encoded_buffer)?;

		let new_payload = bytes::Bytes::copy_from_slice(&encoded_buffer[..size]);

		let new_frame = hang::Frame {
			timestamp: frame.timestamp,
			keyframe: frame.keyframe,
			payload: new_payload,
		};

		output.write(new_frame);
	}

	Ok(())
}
