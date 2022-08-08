use serde::{Deserialize, Serialize};

mod compress;
mod serialization;

use compress::Algo::*;
use serialization::Algo::*;

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct Payload {
    #[serde(skip)]
    pub stream: String,
    pub sequence: u32,
    pub timestamp: u64,
    #[serde(flatten)]
    pub payload: serde_json::Value,
}

#[tokio::main]
async fn main() {
    z().await;
    serde();
}

fn serde() {
    for algo in [Json] {
        let original_payload = Payload::default();

        let compressed_payload = algo.serialize(&original_payload).unwrap();

        let decompressed_payload = algo.deserialize(&compressed_payload).unwrap();
        println!(
            "{:?} \noriginal: {:?}; \ncompressed: {:?}; len: {} \ndecompressed: {:?};\n",
            algo,
            &original_payload,
            &compressed_payload,
            compressed_payload.len(),
            &decompressed_payload,
        );
    }
}

async fn z() {
    for algo in [Lz4, Zlib, Zstd] {
        let original_payload = "Hello World!".as_bytes().to_vec();
        let original_topic = "hello/world".to_owned();

        let mut compressed_payload = original_payload.clone();
        let mut compressed_topic = original_topic.clone();
        algo.compress(&mut compressed_payload, &mut compressed_topic)
            .await
            .unwrap();

        let mut decompressed_payload = compressed_payload.clone();
        let mut decompressed_topic = compressed_topic.clone();
        algo.decompress(&mut decompressed_payload, &mut decompressed_topic)
            .await
            .unwrap();
        println!(
            "{:?} \noriginal: {:?}; topic: {}; len: {} \ncompressed: {:?}; topic: {}; len: {} \ndecompressed: {:?}; topic: {}; len: {}\n",
            algo,
            &original_payload,
            original_topic,
            original_payload.len(),
            &compressed_payload,
            compressed_topic,
            compressed_payload.len(),
            &decompressed_payload,
            decompressed_topic,
            decompressed_payload.len(),
        );
    }
}
