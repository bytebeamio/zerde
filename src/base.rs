use std::{sync::Arc, time::Duration};

use flume::{SendError, Sender};
use log::{info, warn};
use serde::{Deserialize, Serialize};

pub const DEFAULT_TIMEOUT: u64 = 60;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Send error {0}")]
    Send(#[from] SendError<Buffer<Payload>>),
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct SimulatorConfig {
    /// number of devices to be simulated
    pub num_devices: u32,
    /// path to directory containing files with gps paths to be used in simulation
    pub gps_paths: String,
}

pub trait Point: Send + std::fmt::Debug {
    fn sequence(&self) -> u32;
    fn timestamp(&self) -> u64;
}

pub trait Package: Send + std::fmt::Debug {
    fn topic(&self) -> Arc<String>;
    // TODO: Implement a generic Return type that can wrap
    // around custom serialization error types.
    fn serialize(&self) -> serde_json::Result<Vec<u8>>;
    fn anomalies(&self) -> Option<(String, usize)>;
}

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

impl Point for Payload {
    fn sequence(&self) -> u32 {
        self.sequence
    }

    fn timestamp(&self) -> u64 {
        self.timestamp
    }
}

impl Package for Buffer<Payload> {
    fn topic(&self) -> Arc<String> {
        self.topic.clone()
    }

    fn serialize(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(&self.buffer)
    }

    fn anomalies(&self) -> Option<(String, usize)> {
        self.anomalies()
    }
}

#[derive(Debug)]
pub struct Stream {
    name: Arc<String>,
    topic: Arc<String>,
    last_sequence: u32,
    last_timestamp: u64,
    max_buffer_size: usize,
    buffer: Buffer<Payload>,
    tx: Sender<Buffer<Payload>>,
    pub flush_period: Duration,
}

impl Stream {
    pub fn new<S: Into<String>>(
        stream: S,
        topic: S,
        max_buffer_size: usize,
        tx: Sender<Buffer<Payload>>,
    ) -> Stream {
        let name = Arc::new(stream.into());
        let topic = Arc::new(topic.into());
        let buffer = Buffer::new(name.clone(), topic.clone());
        let flush_period = Duration::from_secs(DEFAULT_TIMEOUT);

        Stream {
            name,
            topic,
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size,
            buffer,
            tx,
            flush_period,
        }
    }

    fn add(&mut self, data: Payload) -> Result<Option<Buffer<Payload>>, Error> {
        let current_sequence = data.sequence();
        let current_timestamp = data.timestamp();

        // Fill buffer with data
        self.buffer.buffer.push(data);

        // Anomaly detection
        if current_sequence <= self.last_sequence {
            warn!(
                "Sequence number anomaly! [{}, {}]",
                current_sequence, self.last_sequence
            );
            self.buffer
                .add_sequence_anomaly(self.last_sequence, current_sequence);
        }

        if current_timestamp < self.last_timestamp {
            warn!(
                "Timestamp anomaly!! [{}, {}]",
                current_timestamp, self.last_timestamp
            );
            self.buffer
                .add_timestamp_anomaly(self.last_timestamp, current_timestamp);
        }

        self.last_sequence = current_sequence;
        self.last_timestamp = current_timestamp;

        // if max_buffer_size is breached, flush
        let buf = if self.buffer.buffer.len() >= self.max_buffer_size {
            Some(self.take_buffer())
        } else {
            None
        };

        Ok(buf)
    }

    // Returns buffer content, replacing with empty buffer in-place
    fn take_buffer(&mut self) -> Buffer<Payload> {
        let name = self.name.clone();
        let topic = self.topic.clone();
        info!("Flushing stream name: {}, topic: {}", name, topic);

        std::mem::replace(&mut self.buffer, Buffer::new(name, topic))
    }

    /// Fill buffer with data and trigger async channel send on breaching max_buf_size.
    /// Returns [`StreamStatus`].
    pub async fn fill(&mut self, data: Payload) -> Result<(), Error> {
        if let Some(buf) = self.add(data)? {
            self.tx.send_async(buf).await?;
        }

        Ok(())
    }
}

/// Buffer is an abstraction of a collection that serializer receives.
/// It also contains meta data to understand the type of data
/// e.g stream to mqtt topic mapping
/// Buffer doesn't put any restriction on type of `T`
#[derive(Debug)]
pub struct Buffer<T> {
    pub stream: Arc<String>,
    pub topic: Arc<String>,
    pub buffer: Vec<T>,
    pub anomalies: String,
    pub anomaly_count: usize,
}

impl<T> Buffer<T> {
    pub fn new(stream: Arc<String>, topic: Arc<String>) -> Buffer<T> {
        Buffer {
            stream,
            topic,
            buffer: vec![],
            anomalies: String::with_capacity(100),
            anomaly_count: 0,
        }
    }

    pub fn add_sequence_anomaly(&mut self, last: u32, current: u32) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = String::from(self.stream.as_ref())
            + ".sequence: "
            + &last.to_string()
            + ", "
            + &current.to_string();
        self.anomalies.push_str(&error)
    }

    pub fn add_timestamp_anomaly(&mut self, last: u64, current: u64) {
        self.anomaly_count += 1;
        if self.anomalies.len() >= 100 {
            return;
        }

        let error = "timestamp: ".to_owned() + &last.to_string() + ", " + &current.to_string();
        self.anomalies.push_str(&error)
    }

    pub fn anomalies(&self) -> Option<(String, usize)> {
        if self.anomalies.is_empty() {
            return None;
        }

        Some((self.anomalies.clone(), self.anomaly_count))
    }
}

impl Clone for Stream {
    fn clone(&self) -> Self {
        Stream {
            name: self.name.clone(),
            topic: self.topic.clone(),
            last_sequence: 0,
            last_timestamp: 0,
            max_buffer_size: self.max_buffer_size,
            buffer: Buffer::new(self.buffer.stream.clone(), self.buffer.topic.clone()),
            tx: self.tx.clone(),
            flush_period: self.flush_period,
        }
    }
}
