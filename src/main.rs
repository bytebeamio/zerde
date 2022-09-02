use base::{Payload, SimulatorConfig, Stream};

mod base;
mod compress;
mod serialization;
mod simulator;

use compress::Algo::*;
use flume::bounded;
use log::error;
use prost_reflect::DescriptorPool;
use serialization::{hard_code_proto, Algo::*};

// use crate::serialization::hard_code_avro;

#[tokio::main]
async fn main() {
    let (data_tx, data_rx) = bounded(10);
    std::thread::spawn(|| {
        if let Err(e) = simulator::start(
            data_tx,
            &SimulatorConfig {
                num_devices: 10,
                gps_paths: "./paths".to_string(),
            },
        ) {
            error!("Simulator error: {}", e);
        }
    });

    let descriptor_pool = hard_code_proto();
    // let schema = hard_code_avro();

    loop {
        let next = data_rx.recv_async().await.unwrap();
        let payload = next.buffer;
        let topic = next.topic.as_str();
        serz(&descriptor_pool, topic, payload).await
    }
}

async fn serz(
    descriptor_pool: &DescriptorPool,
    original_topic: &str,
    original_payload: Vec<Payload>,
) {
    // println!(
    //     "Original; payload: {:?}; topic: {}\n",
    //     &original_payload, &original_topic
    // );

    for algo in [
        Json,
        ProtoBuf(descriptor_pool, &format!("test.{}List", original_topic)),
        MessagePack,
        Bson,
        Cbor,
        Pickle,
        // Avro(&schema),
    ] {
        println!("\n------------\n{}\n------------", algo);
        let serialized_payload = algo.serialize(original_payload.clone()).unwrap();

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
    println!("{:?}", algo);

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
    println!("original topic: {}; len: {} \ncompressed topic: {}; len: {} \noriginal == decompressed: {}; same topic: {}; \n",
            original_topic,
            original_payload.len(),
            compressed_topic,
            compressed_payload.len(),
            original_payload == &decompressed_payload,
            original_topic == decompressed_topic,
        );
}
