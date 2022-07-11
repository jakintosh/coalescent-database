use super::{NetworkFrameRead, NetworkFrameWrite};

use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_util::codec::LengthDelimitedCodec;
pub async fn connect(port: u16) -> (NetworkFrameRead, NetworkFrameWrite) {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let stream = TcpStream::connect(address).await.expect("bind tcp");

    let (read, write) = stream.into_split();
    let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
    let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

    (stream, sink)
}
