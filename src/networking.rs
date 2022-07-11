pub mod client;
pub mod server;
pub(crate) mod sink_table;

use crate::engine::{Request, Response};
use serde::{Deserialize, Serialize};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

// type aliases
type NetworkFrameRead = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type NetworkFrameWrite = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

#[derive(Serialize, Deserialize)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}
