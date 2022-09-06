use std::{
    io::{Read, Write},
    time::Instant,
};

use async_compression::tokio::write::{ZlibDecoder, ZlibEncoder, ZstdDecoder, ZstdEncoder};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use tokio::io::AsyncWriteExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::frame::Error),
    #[error("Snap compression error: {0}")]
    Snap(Box<snap::write::IntoInnerError<snap::write::FrameEncoder<Vec<u8>>>>),
}

#[derive(Debug, Clone)]
pub enum Algo {
    Lz4,
    Snappy,
    Zlib,
    Zstd,
}

impl Algo {
    pub async fn compress(&self, payload: &mut Vec<u8>, topic: &mut String) -> Result<u128, Error> {
        let now = Instant::now();
        match self {
            Self::Lz4 => Self::lz4_compress(payload, topic)?,
            Self::Snappy => Self::snappy_compress(payload, topic)?,
            Self::Zlib => Self::zlib_compress(payload, topic).await?,
            Self::Zstd => Self::zstd_compress(payload, topic).await?,
        }

        Ok(now.elapsed().as_micros())
    }

    pub async fn decompress(
        &self,
        payload: &mut Vec<u8>,
        topic: &mut String,
    ) -> Result<u128, Error> {
        let now = Instant::now();
        match self {
            Self::Lz4 => Self::lz4_decompress(payload, topic)?,
            Self::Snappy => Self::snappy_decompress(payload, topic)?,
            Self::Zlib => Self::zlib_decompress(payload, topic).await?,
            Self::Zstd => Self::zstd_decompress(payload, topic).await?,
        }

        Ok(now.elapsed().as_micros())
    }

    fn lz4_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = FrameEncoder::new(vec![]);
        compressor.write_all(payload)?;
        *payload = compressor.finish()?;
        topic.push_str("/lz4");

        Ok(())
    }

    fn snappy_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = snap::write::FrameEncoder::new(vec![]);
        compressor.write_all(payload)?;
        *payload = compressor
            .into_inner()
            .map_err(|e| Error::Snap(Box::new(e)))?;
        topic.push_str("/snappy");

        Ok(())
    }

    async fn zlib_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = ZlibEncoder::new(vec![]);
        compressor.write_all(payload).await?;
        compressor.shutdown().await?;
        *payload = compressor.into_inner();
        topic.push_str("/zlib");

        Ok(())
    }

    async fn zstd_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = ZstdEncoder::new(vec![]);
        compressor.write_all(payload).await?;
        compressor.shutdown().await?;
        *payload = compressor.into_inner();
        topic.push_str("/zstd");

        Ok(())
    }

    fn lz4_decompress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut decompressor = FrameDecoder::new(&payload[..]);
        let mut buffer = vec![];
        decompressor.read_to_end(&mut buffer)?;

        *payload = buffer;
        *topic = topic.replace("/lz4", "");

        Ok(())
    }

    fn snappy_decompress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut decompressor = snap::read::FrameDecoder::new(&payload[..]);
        let mut buffer = vec![];
        decompressor.read_to_end(&mut buffer)?;

        *payload = buffer;
        *topic = topic.replace("/snappy", "");

        Ok(())
    }

    async fn zlib_decompress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut decompressor = ZlibDecoder::new(vec![]);
        decompressor.write_all(payload).await?;
        decompressor.shutdown().await?;
        *payload = decompressor.into_inner();
        *topic = topic.replace("/zlib", "");

        Ok(())
    }

    async fn zstd_decompress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut decompressor = ZstdDecoder::new(vec![]);
        decompressor.write_all(payload).await?;
        decompressor.shutdown().await?;
        *payload = decompressor.into_inner();
        *topic = topic.replace("/zstd", "");

        Ok(())
    }
}
