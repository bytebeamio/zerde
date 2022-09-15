use std::io::{LineWriter, Write};
use std::{collections::HashMap, fs::File};

mod base;
mod compress;
mod serialization;
mod simulator;

use base::{Payload, SimulatorConfig, Stream};
use compress::Algo::*;
use flume::bounded;
use log::error;
use prost_reflect::DescriptorPool;
use serialization::{hard_code_proto, Algo::*};

const MAX_BUF_SIZE: usize = 1; // 10, 100, 1000

// use crate::serialization::hard_code_avro;

#[tokio::main]
async fn main() {
    let (data_tx, data_rx) = bounded(10);
    std::thread::spawn(|| {
        if let Err(e) = simulator::start(
            data_tx,
            &SimulatorConfig {
                num_devices: 1,
                gps_paths: "./paths".to_string(),
            },
        ) {
            error!("Simulator error: {}", e);
        }
    });

    let descriptor_pool = hard_code_proto();
    // let schema = hard_code_avro();

    let mut file_map = HashMap::new();
    std::fs::create_dir_all("./data").unwrap();

    loop {
        let next = data_rx.recv_async().await.unwrap();
        let payload = next.buffer;
        let topic = next.topic.as_str();
        let line = serz(&descriptor_pool, topic, payload).await;
        file_map.entry(topic.to_owned()).or_insert_with(|| {
                let file = File::create(format!("./data/{}_{}.csv", MAX_BUF_SIZE, topic)).unwrap();
                let mut file = LineWriter::new(file);
                eprintln!("{}", topic);
                file.write_all(b"json ser(micros), json len(bytes), json & lz4 #(micros), json & lz4 len(bytes), json & lz4 !(micros), json & snappy #(micros), json & snappy len(bytes), json & snappy !(micros), json & zlib #(micros), json & zlib len(bytes), json & zlib !(micros), json & zstd #(micros), json & zstd len(bytes), json & zstd !(micros), json de(micros), protobuf ser(micros), protobuf len(bytes), protobuf & lz4 #(micros), protobuf & lz4 len(bytes), protobuf & lz4 !(micros), protobuf & snappy #(micros), protobuf & snappy len(bytes), protobuf & snappy !(micros), protobuf & zlib #(micros), protobuf & zlib len(bytes), protobuf & zlib !(micros), protobuf & zstd #(micros), protobuf & zstd len(bytes), protobuf & zstd !(micros), protobuf de(micros), protoref ser(micros), protoref len(bytes), protoref & lz4 #(micros), protoref & lz4 len(bytes), protoref & lz4 !(micros), protoref & snappy #(micros), protoref & snappy len(bytes), protoref & snappy !(micros), protoref & zlib #(micros), protoref & zlib len(bytes), protoref & zlib !(micros), protoref & zstd #(micros), protoref & zstd len(bytes), protoref & zstd !(micros), protoref de(micros), msgpack ser(micros), msgpack len(bytes), msgpack & lz4 #(micros), msgpack & lz4 len(bytes), msgpack & lz4 !(micros), msgpack & snappy #(micros), msgpack & snappy len(bytes), msgpack & snappy !(micros), msgpack & zlib #(micros), msgpack & zlib len(bytes), msgpack & zlib !(micros), msgpack & zstd #(micros), msgpack & zstd len(bytes), msgpack & zstd !(micros), msgpack de(micros), bson ser(micros), bson len(bytes), bson & lz4 #(micros), bson & lz4 len(bytes), bson & lz4 !(micros), bson & snappy #(micros), bson & snappy len(bytes), bson & snappy !(micros), bson & zlib #(micros), bson & zlib len(bytes), bson & zlib !(micros), bson & zstd #(micros), bson & zstd len(bytes), bson & zstd !(micros), bson de(micros), cbor ser(micros), cbor len(bytes), cbor & lz4 #(micros), cbor & lz4 len(bytes), cbor & lz4 !(micros), cbor & snappy #(micros), cbor & snappy len(bytes), cbor & snappy !(micros), cbor & zlib #(micros), cbor & zlib len(bytes), cbor & zlib !(micros), cbor & zstd #(micros), cbor & zstd len(bytes), cbor & zstd !(micros), cbor de(micros), pickle ser(micros), pickle len(bytes), pickle & lz4 #(micros), pickle & lz4 len(bytes), pickle & lz4 !(micros), pickle & snappy #(micros), pickle & snappy len(bytes), pickle & snappy !(micros), pickle & zlib #(micros), pickle & zlib len(bytes), pickle & zlib !(micros), pickle & zstd #(micros), pickle & zstd len(bytes), pickle & zstd !(micros), pickle de(micros),").unwrap();
                file
            }).write_all(line.as_bytes()).unwrap();
    }
}

async fn serz(
    descriptor_pool: &DescriptorPool,
    original_topic: &str,
    original_payload: Vec<Payload>,
) -> String {
    let mut line = "\n".to_string();
    let stream = format!("test.{}List", original_topic);
    for algo in [
        Json,
        Proto(&stream),
        ProtoReflect(descriptor_pool, &stream),
        MessagePack,
        Bson,
        Cbor,
        Pickle,
        // Avro(&schema),
    ] {
        let (serialized_payload, serialization_time) =
            algo.serialize(original_payload.clone()).unwrap();

        line.push_str(&format!(
            "{}, {}, ",
            serialization_time,
            serialized_payload.len()
        ));

        for algo in [Lz4, Snappy, Zlib, Zstd] {
            let (compression_time, compressed_len, decompression_time) =
                z(algo, &serialized_payload, &original_topic).await.unwrap();
            let details = format!(
                "{}, {}, {}, ",
                compression_time, compressed_len, decompression_time
            );
            line.push_str(&details);
        }

        let (_deserialized_payload, deserialization_time) =
            algo.deserialize(&serialized_payload).unwrap();
        line.push_str(&format!("{}, ", deserialization_time,));
    }

    line
}

async fn z(
    algo: compress::Algo,
    original_payload: &Vec<u8>,
    original_topic: &str,
) -> Result<(u128, usize, u128), compress::Error> {
    let mut compressed_payload = original_payload.clone();
    let mut compressed_topic = original_topic.to_owned();
    let compression_time = algo
        .compress(&mut compressed_payload, &mut compressed_topic)
        .await?;

    // println!("compressed: {:?}", compressed_payload);
    let compressed_len = compressed_payload.len();

    let mut decompressed_payload = compressed_payload.clone();
    let mut decompressed_topic = compressed_topic.clone();
    let decompression_time = algo
        .decompress(&mut decompressed_payload, &mut decompressed_topic)
        .await?;

    assert_eq!(original_payload, &decompressed_payload);

    Ok((compression_time, compressed_len, decompression_time))
}
