use crate::engine::{Message, MessageRx, MessageTx, Request, Response};

use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

type SinkTableLock = Arc<Mutex<SinkTable>>;
type NetworkFrameRead = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type NetworkFrameWrite = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

#[derive(Serialize, Deserialize)]
pub enum WireMessage {
    Request(Request),
    Response(Response),
}

struct SinkTable {
    id: usize,
    map: HashMap<usize, NetworkFrameWrite>,
}
impl SinkTable {
    pub fn new() -> SinkTable {
        SinkTable {
            id: 0,
            map: HashMap::new(),
        }
    }
    pub fn insert(&mut self, sink: NetworkFrameWrite) -> usize {
        let id = self.get_next_id();
        self.map.insert(id, sink);
        id
    }
    pub fn get(&mut self, id: usize) -> Option<&mut NetworkFrameWrite> {
        self.map.get_mut(&id)
    }
    pub fn remove(&mut self, id: usize) {
        self.map.remove(&id);
    }

    fn get_next_id(&mut self) -> usize {
        let id = self.id;
        self.id = (std::num::Wrapping(id) + std::num::Wrapping(1)).0;
        id
    }
}

pub struct Client {}
impl Client {
    pub async fn connect(port: u16) -> (NetworkFrameRead, NetworkFrameWrite) {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let stream = TcpStream::connect(address).await.expect("bind tcp");

        let (read, write) = stream.into_split();
        let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
        let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

        (stream, sink)
    }
}

pub struct Server {
    port: u16,
    message_tx: MessageTx,
    message_rx: MessageRx,
    sink_table_lock: SinkTableLock,
}
impl Server {
    pub fn new(port: u16, message_tx: MessageTx, message_rx: MessageRx) -> Server {
        Server {
            port,
            message_tx,
            message_rx,
            sink_table_lock: Arc::new(Mutex::new(SinkTable::new())),
        }
    }

    pub async fn listen(self) {
        let Server {
            port,
            message_tx,
            message_rx,
            sink_table_lock: lock,
        } = self;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(address).await.expect("bind tcp");
        println!("coalescentdb: server started at {}", address);

        tokio::select! {
            _ = Server::accept_connections(listener, lock.clone(), message_tx) => {}
            _ = Server::process_responses(lock.clone(), message_rx) => {}
        };
    }

    async fn accept_connections(
        listener: TcpListener,
        sink_table_lock: SinkTableLock,
        message_tx: MessageTx,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("accepted connection from {}", addr);

                    let (read, write) = stream.into_split();
                    let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
                    let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

                    let message_tx = message_tx.clone();
                    let sink_id;
                    {
                        sink_id = sink_table_lock.lock().unwrap().insert(sink);
                    }

                    tokio::spawn(Server::handle_connection(
                        stream,
                        sink_id,
                        sink_table_lock.clone(),
                        message_tx,
                    ));
                }
                Err(e) => println!("couldn't accept client: {:?}", e),
            }
        }
    }

    async fn handle_connection(
        mut stream: NetworkFrameRead,
        sink_id: usize,
        sink_table_lock: SinkTableLock,
        message_tx: MessageTx,
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
            let wire_message: WireMessage = match rmp_serde::from_slice(bytes.as_ref()) {
                Ok(r) => r,
                Err(e) => {
                    println!("msgpack deserialization fail: {:?}", e);
                    continue;
                }
            };

            // process the wire message
            match wire_message {
                // if request, pass to engine
                WireMessage::Request(request) => {
                    let message = Message::Request { sink_id, request };
                    if let Err(_) = message_tx.send(message) {
                        // channel is closed, close the connection
                        break;
                    }
                }
                _ => {}
            }
        }

        // connection is closed, drop the sink
        sink_table_lock.lock().unwrap().remove(sink_id);
        println!("coalescentdb: closing connection @ id: {}", sink_id);
    }

    async fn process_responses(sink_table_lock: SinkTableLock, mut message_rx: MessageRx) {
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

                    {
                        let mut sink_table = sink_table_lock.lock().unwrap();
                        let sink = match sink_table.get(sink_id) {
                            Some(s) => s,
                            None => {
                                println!("can't send response, sink not found");
                                continue;
                            }
                        };
                        if let Err(_) = sink.send(bytes.into()).await {};
                    }
                }
                _ => {}
            };
        }
    }
}
