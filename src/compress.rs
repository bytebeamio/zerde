use std::io::{Read, Write};

use async_compression::tokio::write::{ZlibDecoder, ZlibEncoder, ZstdDecoder, ZstdEncoder};
use lz4_flex::frame::{FrameDecoder, FrameEncoder};
use tokio::io::AsyncWriteExt;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Io error {0}")]
    Io(#[from] std::io::Error),
    #[error("LZ4 compression error: {0}")]
    Lz4(#[from] lz4_flex::frame::Error),
}

#[derive(Debug, Clone)]
pub enum Algo {
    Lz4,
    Zlib,
    Zstd,
}

impl Algo {
    pub async fn compress(&self, payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        match self {
            Self::Lz4 => Self::lz4_compress(payload, topic),
            Self::Zlib => Self::zlib_compress(payload, topic).await,
            Self::Zstd => Self::zstd_compress(payload, topic).await,
        }
    }

    pub async fn decompress(&self, payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        match self {
            Self::Lz4 => Self::lz4_decompress(payload, topic),
            Self::Zlib => Self::zlib_decompress(payload, topic).await,
            Self::Zstd => Self::zstd_decompress(payload, topic).await,
        }
    }

    fn lz4_compress(payload: &mut Vec<u8>, topic: &mut String) -> Result<(), Error> {
        let mut compressor = FrameEncoder::new(vec![]);
        compressor.write_all(payload)?;
        *payload = compressor.finish()?;
        topic.push_str("/lz4");

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