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
    println!("data, json ser(micros), json len(bytes), json & lz4 #(micros), json & lz4 len(bytes), json & lz4 !(micros), json & snappy #(micros), json & snappy len(bytes), json & snappy !(micros), json & zlib #(micros), json & zlib len(bytes), json & zlib !(micros), json & zstd #(micros), json & zstd len(bytes), json & zstd !(micros), json de(micros), protobuf ser(micros), protobuf len(bytes), protobuf & lz4 #(micros), protobuf & lz4 len(bytes), protobuf & lz4 !(micros), protobuf & snappy #(micros), protobuf & snappy len(bytes), protobuf & snappy !(micros), protobuf & zlib #(micros), protobuf & zlib len(bytes), protobuf & zlib !(micros), protobuf & zstd #(micros), protobuf & zstd len(bytes), protobuf & zstd !(micros), protobuf de(micros), msgpack ser(micros), msgpack len(bytes), msgpack & lz4 #(micros), msgpack & lz4 len(bytes), msgpack & lz4 !(micros), msgpack & snappy #(micros), msgpack & snappy len(bytes), msgpack & snappy !(micros), msgpack & zlib #(micros), msgpack & zlib len(bytes), msgpack & zlib !(micros), msgpack & zstd #(micros), msgpack & zstd len(bytes), msgpack & zstd !(micros), msgpack de(micros), bson ser(micros), bson len(bytes), bson & lz4 #(micros), bson & lz4 len(bytes), bson & lz4 !(micros), bson & snappy #(micros), bson & snappy len(bytes), bson & snappy !(micros), bson & zlib #(micros), bson & zlib len(bytes), bson & zlib !(micros), bson & zstd #(micros), bson & zstd len(bytes), bson & zstd !(micros), bson de(micros), cbor ser(micros), cbor len(bytes), cbor & lz4 #(micros), cbor & lz4 len(bytes), cbor & lz4 !(micros), snappy #(micros), snappy len(bytes), snappy !(micros), zlib #(micros), zlib len(bytes), zlib !(micros), zstd #(micros), zstd len(bytes), zstd !(micros), cbor de(micros),pickle ser(micros), pickle len(bytes), lz4 #(micros), lz4 len(bytes), lz4 !(micros), snappy #(micros), snappy len(bytes), snappy !(micros), zlib #(micros), zlib len(bytes), zlib !(micros), zstd #(micros), zstd len(bytes), zstd !(micros), pickle de(micros),");

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
    print!("{}, ", &original_topic);

    for algo in [
        Json,
        ProtoBuf(descriptor_pool, &format!("test.{}List", original_topic)),
        MessagePack,
        Bson,
        Cbor,
        Pickle,
        // Avro(&schema),
    ] {
        let serialized_payload = algo.serialize(original_payload.clone()).unwrap();

        print!("{}, ", serialized_payload.len(),);

        for algo in [Lz4, Snappy, Zlib, Zstd] {
            z(algo, &serialized_payload, &original_topic).await;
        }

        let _ = algo.deserialize(&serialized_payload).unwrap();
    }
    println!();
}

async fn z(algo: compress::Algo, original_payload: &Vec<u8>, original_topic: &str) {
    let mut compressed_payload = original_payload.clone();
    let mut compressed_topic = original_topic.to_owned();
    algo.compress(&mut compressed_payload, &mut compressed_topic)
        .await
        .unwrap();

    // println!("compressed: {:?}", compressed_payload);
    print!("{}, ", compressed_payload.len());

    let mut decompressed_payload = compressed_payload.clone();
    let mut decompressed_topic = compressed_topic.clone();
    algo.decompress(&mut decompressed_payload, &mut decompressed_topic)
        .await
        .unwrap();
}
