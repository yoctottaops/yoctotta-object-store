use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

use crate::error::OrionError;

/// A stream of byte chunks for reading/writing object data.
/// This avoids loading entire objects into memory.
pub type DataStream =
    Pin<Box<dyn Stream<Item = std::result::Result<Bytes, OrionError>> + Send + 'static>>;

/// Helper to create a DataStream from a single Bytes value.
pub fn single_chunk_stream(data: Bytes) -> DataStream {
    Box::pin(futures::stream::once(async move { Ok(data) }))
}

/// Helper to create a DataStream from a Vec<u8>.
pub fn vec_to_stream(data: Vec<u8>) -> DataStream {
    single_chunk_stream(Bytes::from(data))
}

/// Helper to create an empty DataStream.
pub fn empty_stream() -> DataStream {
    Box::pin(futures::stream::empty())
}

/// Collect a DataStream into a single Vec<u8>.
/// Use with caution on large objects — this loads everything into memory.
pub async fn collect_stream(mut stream: DataStream) -> crate::Result<Vec<u8>> {
    use futures::StreamExt;

    let mut buf = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}
