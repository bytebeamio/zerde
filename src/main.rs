use serde::{Deserialize, Serialize};

mod compress;
mod serialization;

use compress::Algo::*;
use serde_json::json;
use serialization::{hard_code_proto, Algo::*};

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
    let original_payload = Payload {
        stream: "test.can".to_string(),
        sequence: 123,
        timestamp: 123,
        payload: json!({
            "data": 100
        }),
    };
    let original_topic = "hello/world".to_owned();
    println!(
        "Original; payload: {:?}; topic: {}\n",
        &original_payload, &original_topic
    );

    let descriptor_pool = hard_code_proto();
    for algo in [Json, ProtoBuf(&descriptor_pool, "test.can"), MessagePack, Bson, Cbor, Pickle] {
        println!("------------\n{}\n------------\n", algo);
        let serialized_payload = algo.serialize(&original_payload).unwrap();

        println!(
            "serialized: {:?}; len: {}\n",
            &serialized_payload,
            serialized_payload.len(),
        );

        for algo in [Lz4, Zlib, Zstd] {
            z(algo, &serialized_payload, &original_topic).await;
        }

        let deserialized_payload = algo.deserialize(&serialized_payload).unwrap();
        println!("deserialized: {:?};", &deserialized_payload,);
    }
}

async fn z(algo: compress::Algo, original_payload: &Vec<u8>, original_topic: &str) {
    let mut compressed_payload = original_payload.clone();
    let mut compressed_topic = original_topic.to_owned();
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
