use std::{
    fmt::Display,
    io::{Read, Write},
    time::Instant,
};

// use apache_avro::{from_value, to_value, Reader, Schema, Writer};
use prost_reflect::{prost::Message, DescriptorPool, DynamicMessage, SerializeOptions};
use serde::{Deserialize, Serialize};
use serde_json::{Deserializer, Serializer};
use serde_pickle::{DeOptions, SerOptions};

mod capnproto;
mod proto;

use crate::Payload;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    // #[error("Avro serialization error {0}")]
    // Avro(#[from] apache_avro::Error),
    // #[error("Avro serialization missing element")]
    // AvroMissing,
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
    #[error("Capn error: {0}")]
    Capn(#[from] capnp::Error),
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

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
struct PayloadArray {
    messages: Vec<Payload>,
}

#[derive(Debug, Clone)]
pub enum Algo<'a> {
    Bson,
    Cbor,
    Json,
    MessagePack,
    Pickle,
    Proto(&'a str),
    ProtoReflect(&'a DescriptorPool, &'a str),
    Capn(&'a str),
}

impl Display for Algo<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProtoReflect(_, s) => f.write_fmt(format_args!("ProtoBuf: {}", s)),
            a => f.write_fmt(format_args!("{:?}", a)),
        }
    }
}

impl<'a> Algo<'a> {
    pub fn serialize(&self, payload: Vec<Payload>) -> Result<(Vec<u8>, u128), Error> {
        let now = Instant::now();
        let serialized = match self {
            // Self::Avro(schema) => self.avro_serialize(payload, schema),
            Self::Bson => self.bson_serialize(payload)?,
            Self::Capn(stream) => capnproto::serialize(payload, stream)?,
            Self::Cbor => self.cbor_serialize(payload)?,
            Self::Json => self.json_serialize(payload)?,
            Self::MessagePack => self.msgpck_serialize(payload)?,
            Self::Pickle => self.pickle_serialize(payload)?,
            Self::Proto(stream) => self.proto_serialize(payload, stream)?,
            Self::ProtoReflect(descriptor_pool, stream) => {
                self.proto_reflect_serialize(descriptor_pool, payload, stream)?
            }
        };
        let serialization_time = now.elapsed().as_micros();

        Ok((serialized, serialization_time))
    }

    pub fn deserialize(&self, payload: &[u8]) -> Result<(Vec<Payload>, u128), Error> {
        let now = Instant::now();
        let deserialized = match self {
            // Self::Avro(schema) => self.avro_deserialize(payload, schema),
            Self::Bson => self.bson_deserialize(payload)?,
            Self::Capn(stream) => capnproto::deserialize(payload, stream)?,
            Self::Cbor => self.cbor_deserialize(payload)?,
            Self::Json => self.json_deserialize(payload)?,
            Self::MessagePack => self.msgpck_deserialize(payload)?,
            Self::Pickle => self.pickle_deserialize(payload)?,
            Self::Proto(stream) => self.proto_deserialize(payload, stream)?,
            Self::ProtoReflect(descriptor_pool, stream) => {
                self.proto_reflect_deserialize(descriptor_pool, payload, stream)?
            }
        };
        let deserialization_time = now.elapsed().as_micros();

        Ok((deserialized, deserialization_time))
    }

    // fn avro_serialize(&self, payload: Vec<Payload>, schema: &Schema) -> Result<Vec<u8>, Error> {
    //     let mut serialized = vec![];
    //     let mut writer = Writer::new(schema, &mut serialized);
    //     let value = to_value(payload)?;
    //     writer.append(value)?;
    //     writer.flush()?;

    //     Ok(serialized)
    // }

    fn bson_serialize(&self, payload: Vec<Payload>) -> Result<Vec<u8>, Error> {
        let array = PayloadArray {
            messages: payload.to_owned(),
        };
        let serialized = bson::to_vec(&array)?;

        Ok(serialized)
    }

    fn cbor_serialize(&self, payload: Vec<Payload>) -> Result<Vec<u8>, Error> {
        let mut serialized = vec![];
        ciborium::ser::into_writer(&payload, &mut serialized)?;

        Ok(serialized)
    }

    fn json_serialize(&self, payload: Vec<Payload>) -> Result<Vec<u8>, Error> {
        let serialized = serde_json::to_vec(&payload)?;

        Ok(serialized)
    }

    fn msgpck_serialize(&self, payload: Vec<Payload>) -> Result<Vec<u8>, Error> {
        let serialized = rmp_serde::to_vec(&payload)?;

        Ok(serialized)
    }

    fn pickle_serialize(&self, payload: Vec<Payload>) -> Result<Vec<u8>, Error> {
        let serialized = serde_pickle::to_vec(&payload, SerOptions::new())?;

        Ok(serialized)
    }

    fn proto_serialize(&self, payload: Vec<Payload>, stream: &str) -> Result<Vec<u8>, Error> {
        proto::serialize(payload, stream)
    }

    fn proto_reflect_serialize(
        &self,
        descriptor_pool: &DescriptorPool,
        payload: Vec<Payload>,
        stream: &str,
    ) -> Result<Vec<u8>, Error> {
        let desc = descriptor_pool.get_message_by_name(stream).unwrap();
        let payload = PayloadArray {
            messages: payload.to_owned(),
        };
        let json_serialized = serde_json::to_vec(&payload)?;

        let mut deserializer = Deserializer::from_slice(&json_serialized);
        let msg = DynamicMessage::deserialize(desc, &mut deserializer)?;

        let mut serialized = vec![];
        msg.encode(&mut serialized)?;

        Ok(serialized)
    }

    // fn avro_deserialize(&self, payload: &[u8], schema: &Schema) -> Result<Vec<Payload>, Error> {
    //     let mut reader = Reader::with_schema(schema, payload)?;
    //     let value = reader.next().ok_or(Error::AvroMissing)??;
    //     let deserialized = from_value(&value)?;

    //     Ok(deserialized)
    // }

    fn bson_deserialize(&self, payload: &[u8]) -> Result<Vec<Payload>, Error> {
        let deserialized: PayloadArray = bson::from_slice(payload)?;

        Ok(deserialized.messages)
    }

    fn cbor_deserialize(&self, payload: &[u8]) -> Result<Vec<Payload>, Error> {
        let deserialized = ciborium::de::from_reader(payload)?;

        Ok(deserialized)
    }

    fn json_deserialize(&self, payload: &[u8]) -> Result<Vec<Payload>, Error> {
        let deserialized = serde_json::from_slice(payload)?;

        Ok(deserialized)
    }

    fn msgpck_deserialize(&self, payload: &[u8]) -> Result<Vec<Payload>, Error> {
        let deserialized = rmp_serde::from_slice(payload)?;

        Ok(deserialized)
    }

    fn pickle_deserialize(&self, payload: &[u8]) -> Result<Vec<Payload>, Error> {
        let deserialized = serde_pickle::from_slice(payload, DeOptions::new())?;

        Ok(deserialized)
    }

    fn proto_deserialize(&self, payload: &[u8], stream: &str) -> Result<Vec<Payload>, Error> {
        proto::deserialize(payload, stream)
    }

    fn proto_reflect_deserialize(
        &self,
        descriptor_pool: &DescriptorPool,
        payload: &[u8],
        stream: &str,
    ) -> Result<Vec<Payload>, Error> {
        let desc = descriptor_pool.get_message_by_name(stream).unwrap();

        let deserialized = DynamicMessage::decode(desc, payload)?;
        let options = SerializeOptions::new().stringify_64_bit_integers(false);
        let mut json_serialized = vec![];
        let mut serializer = Serializer::new(&mut json_serialized);
        deserialized.serialize_with_options(&mut serializer, &options)?;
        let array: PayloadArray = serde_json::from_slice(&json_serialized)?;

        Ok(array.messages)
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
            double longitude = 1;
            double latitude = 2;
            uint64 timestamp = 3;
            sint32 sequence = 4;
        }
        
        message gpsList {
            repeated gps messages = 1;
        }

        message bms {
            sint32 sequence = 1;
            uint64 timestamp = 2;
            int32 periodicity_ms = 3;
            double mosfet_temperature = 4;
            double ambient_temperature = 5;
            int32 mosfet_status = 6;
            int32 cell_voltage_count = 7;
            double cell_voltage_1 = 8;
            double cell_voltage_2 = 9;
            double cell_voltage_3 = 10;
            double cell_voltage_4 = 11;
            double cell_voltage_5 = 12;
            double cell_voltage_6 = 13;
            double cell_voltage_7 = 14;
            double cell_voltage_8 = 15;
            double cell_voltage_9 = 16;
            double cell_voltage_10 = 17;
            double cell_voltage_11 = 18;
            double cell_voltage_12 = 19;
            double cell_voltage_13 = 20;
            double cell_voltage_14 = 21;
            double cell_voltage_15 = 22;
            double cell_voltage_16 = 23;
            int32 cell_thermistor_count = 24;
            double cell_temp_1 = 25;
            double cell_temp_2 = 26;
            double cell_temp_3 = 27;
            double cell_temp_4 = 28;
            double cell_temp_5 = 29;
            double cell_temp_6 = 30;
            double cell_temp_7 = 31;
            double cell_temp_8 = 32;
            int32 cell_balancing_status = 33;
            double pack_voltage = 34;
            double pack_current = 35;
            double pack_soc = 36;
            double pack_soh = 37;
            double pack_sop = 38;
            int64 pack_cycle_count = 39;
            int64 pack_available_energy = 40;
            int64 pack_consumed_energy = 41;
            int32 pack_fault = 42;
            int32 pack_status = 43;
        }
        
        message bmsList {
            repeated bms messages = 1;
        }

        message peripherals {
            string gps = 1;
            string gsm = 2;
            string imu = 3;
            string left_indicator = 4;
            string right_indicator = 5;
            string headlamp = 6;
            string horn = 7;
            string left_brake = 8;
            string right_brake = 9;
            sint32 sequence = 10;
            uint64 timestamp = 11;
        }

        message peripheralsList {
            repeated peripherals messages = 1;
        }

        message shadow {
            string mode = 1;
            string status = 2;
            string firmware_version = 3;
            string config_version = 4;
            int64 distance_travelled = 5;
            int64 range = 6;
            double SOC = 7;
            sint32 sequence = 8;
            uint64 timestamp = 9;
        }

        message shadowList {
            repeated shadow messages = 1;
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

// TODO: replace type information with following
// "type": "record",
// "name": "can",
// "fields" : [
//     {"name": "sequence", "type": "int"},
//     {"name": "timestamp", "type": "long"},
//     {"name": "data", "type": "long"}
// ]
// pub fn hard_code_avro() -> Schema {
//     Schema::parse_str(
//         r##"
//     {
//         "namespace": "test",
//         "type": "array",
//         "items": {
//             "type": "map",
//             "values": "long"
//         }
//     }
// "##,
//     )
//     .unwrap()
// }
