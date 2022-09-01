use serde::{Deserialize, Serialize};

mod compress;
mod serialization;

use compress::Algo::*;
use serde_json::json;
use serialization::{hard_code_proto, Algo::*};

// use crate::serialization::hard_code_avro;

// TODO Don't do any deserialization on payload. Read it a Vec<u8> which is in turn a json
// TODO which cloud will double deserialize (Batch 1st and messages next)
#[derive(Debug, Serialize, Deserialize, Default, Clone, PartialEq, Eq)]
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
    let mut original_payload = vec![];
    for i in 1..101 {
        original_payload.push(Payload {
            stream: String::new(),
            sequence: i,
            timestamp: i as u64,
            payload: json!({ "data": i * 100 + i }),
        });
    }
    let original_topic = "hello/world".to_owned();
    // println!(
    //     "Original; payload: {:?}; topic: {}\n",
    //     &original_payload, &original_topic
    // );

    let descriptor_pool = hard_code_proto();
    // let schema = hard_code_avro();

    for algo in [
        Json,
        ProtoBuf(&descriptor_pool, "test.canList"),
        MessagePack,
        Bson,
        Cbor,
        Pickle,
        // Avro(&schema),
    ] {
        println!("\n------------\n{}\n------------", algo);
        let serialized_payload = algo.serialize(&original_payload).unwrap();

        // println!(
        //     "serialized: {:?}; len: {}\n",
        //     &serialized_payload,
        //     serialized_payload.len(),
        // );

        for algo in [Lz4, Snappy, Zlib, Zstd] {
            z(algo, &serialized_payload, &original_topic).await;
        }

        let deserialized_payload = algo.deserialize(&serialized_payload).unwrap();
        println!(
            "original == deserialized: {:?};",
            original_payload == deserialized_payload,
        );
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
    // println!("compressed: {:?}", compressed_payload);
    println!(
            "{:?} \noriginal topic: {}; len: {} \ncompressed topic: {}; len: {} \noriginal == decompressed: {}; same topic: {}; \n",
            algo,
            original_topic,
            original_payload.len(),
            compressed_topic,
            compressed_payload.len(),
            original_payload == &decompressed_payload,
            original_topic == decompressed_topic,
        );
}
