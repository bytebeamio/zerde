use std::{
    fmt::Display,
    io::{Read, Write},
};

use prost_reflect::{prost::Message, DescriptorPool, DynamicMessage, SerializeOptions};
use serde_json::{Deserializer, Serializer, Value};
use serde_pickle::{DeOptions, SerOptions};

use crate::Payload;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Bson serialization error {0}")]
    BsonSer(#[from] bson::ser::Error),
    #[error("Bson deserialization error {0}")]
    BsonDe(#[from] bson::de::Error),
    #[error("Ciborium serialization error {0}")]
    CiboriumSer(#[from] ciborium::ser::Error<std::io::Error>),
    #[error("Ciborium deserialization error {0}")]
    CiboriumDe(#[from] ciborium::de::Error<std::io::Error>),
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("Pickle error: {0}")]
    Pickle(#[from] serde_pickle::Error),
    #[error("RMP Encode error: {0}")]
    RmpEncode(#[from] rmp_serde::encode::Error),
    #[error("RMP Decode error: {0}")]
    RmpDecode(#[from] rmp_serde::decode::Error),
    #[error("Prost reflect descriptor error = {0}")]
    ProstDescriptor(#[from] prost_reflect::DescriptorError),
    #[error("Prost reflect encode error = {0}")]
    ProstEncode(#[from] prost_reflect::prost::EncodeError),
    #[error("Prost reflect decode error = {0}")]
    ProstDecode(#[from] prost_reflect::prost::DecodeError),
}

#[derive(Debug, Clone)]
pub enum Algo<'a> {
    // Avro,
    Bson,
    Cbor,
    Json,
    // Marshal,
    MessagePack,
    Pickle,
    ProtoBuf(&'a DescriptorPool, &'a str),
    // Thrift,
    // Ujson,
}

impl Display for Algo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtoBuf(_, s) => f.write_fmt(format_args!("ProtoBuf: {}", s)),
            a => f.write_fmt(format_args!("{:?}", a)),
        }
    }
}

impl<'a> Algo<'a> {
    pub fn serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        match self {
            Self::Bson => self.bson_serialize(payload),
            Self::Cbor => self.cbor_serialize(payload),
            Self::Json => self.json_serialize(payload),
            Self::MessagePack => self.msgpck_serialize(payload),
            Self::Pickle => self.pickle_serialize(payload),
            Self::ProtoBuf(descriptor_pool, stream) => {
                self.proto_serialize(descriptor_pool, payload, stream)
            }
        }
    }

    pub fn deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        match self {
            Self::Bson => self.bson_deserialize(payload),
            Self::Cbor => self.cbor_deserialize(payload),
            Self::Json => self.json_deserialize(payload),
            Self::MessagePack => self.msgpck_deserialize(payload),
            Self::Pickle => self.pickle_deserialize(payload),
            Self::ProtoBuf(descriptor_pool, stream) => {
                self.proto_deserialize(descriptor_pool, payload, stream)
            }
        }
    }

    fn bson_serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        let serialized = bson::to_vec(payload)?;

        Ok(serialized)
    }

    fn cbor_serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        let mut serialized = vec![];
        ciborium::ser::into_writer(payload, &mut serialized)?;

        Ok(serialized)
    }

    fn json_serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        let serialized = serde_json::to_vec(payload)?;

        Ok(serialized)
    }

    fn msgpck_serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        let serialized = rmp_serde::to_vec(payload)?;

        Ok(serialized)
    }

    fn pickle_serialize(&self, payload: &Payload) -> Result<Vec<u8>, Error> {
        let serialized = serde_pickle::to_vec(payload, SerOptions::new())?;

        Ok(serialized)
    }

    fn proto_serialize(
        &self,
        descriptor_pool: &DescriptorPool,
        payload: &Payload,
        stream: &str,
    ) -> Result<Vec<u8>, Error> {
        let desc = descriptor_pool.get_message_by_name(stream).unwrap();
        let json_serialized = self.json_serialize(payload)?;

        let mut deserializer = Deserializer::from_slice(&json_serialized);
        let msg = DynamicMessage::deserialize(desc, &mut deserializer)?;

        let mut serialized = vec![];
        msg.encode(&mut serialized)?;

        Ok(serialized)
    }

    fn bson_deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        let deserialized = bson::from_slice(payload)?;

        Ok(deserialized)
    }

    fn cbor_deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        let deserialized = ciborium::de::from_reader(&payload[..])?;

        Ok(deserialized)
    }

    fn json_deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        let deserialized = serde_json::from_slice(payload)?;

        Ok(deserialized)
    }

    fn msgpck_deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        let deserialized = rmp_serde::from_slice(payload)?;

        Ok(deserialized)
    }

    fn pickle_deserialize(&self, payload: &Vec<u8>) -> Result<Payload, Error> {
        let deserialized = serde_pickle::from_slice(payload, DeOptions::new())?;

        Ok(deserialized)
    }

    fn proto_deserialize(
        &self,
        descriptor_pool: &DescriptorPool,
        payload: &Vec<u8>,
        stream: &str,
    ) -> Result<Payload, Error> {
        let desc = descriptor_pool.get_message_by_name(stream).unwrap();

        let deserialized = DynamicMessage::decode(desc, &payload[..])?;
        let options = SerializeOptions::new().stringify_64_bit_integers(false);
        let mut json_serialized = vec![];
        let mut serializer = Serializer::new(&mut json_serialized);
        deserialized.serialize_with_options(&mut serializer, &options)?;

        let json: Value = serde_json::from_slice(&json_serialized)?;

        let mut deserialized: Payload = serde_json::from_value(json)?;
        deserialized.stream = stream.to_string();

        Ok(deserialized)
    }
}

pub fn hard_code_proto() -> DescriptorPool {
    let schema = r#"syntax = "proto3";
        package test;
        
        message imu {
            uint64 timestamp  = 1;
            uint32 sequence = 2;
            double ax = 3;
            double ay = 4;
            double az = 5;
            double pitch = 6;
            double roll = 7;
            double yaw = 8;
            double magx = 9;
            double magy = 10;
            double magz = 11;
        }
        
        message imuList {
            repeated imu messages = 1;
        }
        
        message motor {
            uint64 timestamp  = 1;
            uint32 sequence = 2;
            double temperature1 = 3;
            double temperature2 = 4;
            double temperature3 = 5;
            double voltage = 6;
            double current = 7;
            uint32 rpm = 8;
        }
        
        message motorList {
            repeated motor messages = 1;
        }
        
        message can {
            uint64 timestamp = 1;
            sint32 sequence = 2;
            uint64 data = 3;
        }
        
        message canList {
            repeated can messages = 1;
        }
        
        message gps {
            double lon = 1;
            double lat = 2;
            uint64 timestamp = 3;
            sint32 sequence = 4;
        }
        
        message gpsList {
            repeated gps messages = 1;
        }
        "#
    .to_string();

    let proto_dir = "/tmp/beamd/protos";
    let proto_file_path = proto_dir.to_owned() + "/schema.proto";
    let descriptor_file_path = proto_dir.to_owned() + "/file_descriptor_set.bin";
    std::fs::create_dir_all(proto_dir).unwrap();
    let mut file = std::fs::File::create(&proto_file_path).unwrap();
    file.write_all(schema.as_bytes()).unwrap();

    std::env::set_var("OUT_DIR", proto_dir);
    prost_reflect_build::Builder::new()
        .file_descriptor_set_path(&descriptor_file_path)
        .compile_protos(&[proto_file_path], &[proto_dir])
        .expect("Failed to compile protos");

    let mut descriptor_file = std::fs::File::open(descriptor_file_path).unwrap();
    let mut file_descriptor_set = vec![];
    descriptor_file
        .read_to_end(&mut file_descriptor_set)
        .unwrap();

    DescriptorPool::decode(file_descriptor_set.as_slice()).unwrap()
}
