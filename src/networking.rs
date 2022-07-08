use crate::engine::{Message, MessageRx, MessageTx, Request, Response};

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream,
    },
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

// type aliases
type NetworkFrameRead = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type NetworkFrameWrite = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

#[derive(Serialize, Deserialize)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}

pub async fn connect(port: u16) -> (NetworkFrameRead, NetworkFrameWrite) {
    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let stream = TcpStream::connect(address).await.expect("bind tcp");

    let (read, write) = stream.into_split();
    let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
    let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

    (stream, sink)
}

enum SinkTableMessage {
    Insert {
        sink: NetworkFrameWrite,
        id_tx: oneshot::Sender<usize>,
    },
    Remove(usize),
    Send {
        id: usize,
        bytes: Bytes,
    },
}

type SinkMessageTx = UnboundedSender<SinkTableMessage>;
type SinkMessageRx = UnboundedReceiver<SinkTableMessage>;

async fn run_sink_table(mut sink_message_rx: SinkMessageRx) {
    let mut id = 0usize;
    let mut map = HashMap::new();

    while let Some(message) = sink_message_rx.recv().await {
        match message {
            SinkTableMessage::Insert { sink, id_tx } => {
                let sink_id = id;
                id = (std::num::Wrapping(id) + std::num::Wrapping(1)).0;
                map.insert(sink_id, sink);
                id_tx.send(id);
            }
            SinkTableMessage::Remove(sink_id) => {
                map.remove(&sink_id);
            }
            SinkTableMessage::Send { id: sink_id, bytes } => {
                let sink = match map.get_mut(&sink_id) {
                    Some(s) => s,
                    None => {
                        println!("can't send response, sink not found");
                        continue;
                    }
                };
                if let Err(_) = sink.send(bytes.into()).await {};
            }
        }
    }
}

pub async fn listen(port: u16, message_tx: MessageTx, message_rx: MessageRx) {
    let (sink_message_tx, sink_message_rx) = mpsc::unbounded_channel();

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(address).await.expect("bind tcp");
    println!("coalescentdb: server started at {}", address);

    tokio::select! {
        _ = accept_connections(listener, message_tx, sink_message_tx.clone()) => {}
        _ = poll_engine_messages(message_rx, sink_message_tx) => {}
    };
}

async fn accept_connections(
    listener: TcpListener,
    message_tx: MessageTx,
    sink_message_tx: SinkMessageTx,
) {
    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("accepted connection from {}", addr);

                // split and frame the tcp stream
                let (read, write) = stream.into_split();
                let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
                let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

                // prepare data to be moved into tokio task
                let (id_tx, id_rx) = oneshot::channel();
                if let Err(_) = sink_message_tx.send(SinkTableMessage::Insert { sink, id_tx }) {
                    println!("sink table is closed, cannot accept any more connections, aborting");
                    return;
                }
                let sink_id = match id_rx.await {
                    Ok(id) => id,
                    Err(e) => {
                        println!("couldn't store connection sink");
                        continue;
                    }
                };
                let message_tx = message_tx.clone();
                tokio::spawn(poll_connection(
                    stream,
                    message_tx,
                    sink_id,
                    sink_message_tx.clone(),
                ));
            }
            Err(e) => println!("couldn't accept client: {:?}", e),
        }
    }
}

async fn poll_connection(
    mut stream: NetworkFrameRead,
    message_tx: MessageTx,
    sink_id: usize,
    sink_message_tx: SinkMessageTx,
) {
    while let Some(frame) = stream.next().await {
        // decode the tcp frame
        let bytes = match frame {
            Ok(b) => b,
            Err(e) => {
                println!("tcp frame decode failure: {:?}", e);
                continue;
            }
        };

        // deserialize the frame bytes
        let wire_message = match rmp_serde::from_slice(bytes.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                println!("msgpack deserialization fail: {:?}", e);
                continue;
            }
        };

        // handle the wire message
        match wire_message {
            WireMessage::Request(request) => {
                match message_tx.send(Message::Request { sink_id, request }) {
                    Err(e) => println!("messsage channel was closed: {}", e),
                    _ => {}
                }
            }
            WireMessage::Response(_) => {
                println!("server unexpectedly received 'response' message");
            }
        };
    }

    // connection is closed, drop the sink
    sink_message_tx.send(SinkTableMessage::Remove(sink_id));
    println!("coalescentdb: closing connection @ id: {}", sink_id);
}

async fn poll_engine_messages(mut message_rx: MessageRx, sink_message_tx: SinkMessageTx) {
    while let Some(message) = message_rx.recv().await {
        match message {
            Message::Response { sink_id, response } => {
                let wire_message = WireMessage::Response(response);
                let bytes = match rmp_serde::to_vec(&wire_message) {
                    Ok(b) => b,
                    Err(e) => {
                        println!("msgpack serialization fail: {:?}", e);
                        continue;
                    }
                };

                sink_message_tx.send(SinkTableMessage::Send {
                    id: sink_id,
                    bytes: bytes.into(),
                });
            }
            _ => {}
        };
    }
}
