use crate::Payload;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("Json error: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Debug, Clone)]
pub enum Algo {
    // Avro,
    // Bson,
    // Cbor,
    Json,
    // Marshal,
    // MessagePack,
    // Pickle,
    // ProtoBuf,
    // Thrift,
    // Ujson,
}

impl Algo {
    pub fn serialize(&self, payload: &Payload) -> Result<String, Error> {
        match self {
            Self::Json => Self::json_serialize(payload),
        }
    }

    pub fn deserialize(&self, payload: &String) -> Result<Payload, Error> {
        match self {
            Self::Json => Self::json_deserialize(payload),
        }
    }

    fn json_serialize(payload: &Payload) -> Result<String, Error> {
        let serialized = serde_json::to_string(payload)?;

        Ok(serialized)
    }

    fn json_deserialize(payload: &String) -> Result<Payload, Error> {
        let deserialized = serde_json::from_str(payload)?;

        Ok(deserialized)
    }
}
