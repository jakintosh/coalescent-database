use crate::engine::{Request, RequestDesc, RequestTx, Response, ResponseRx};

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

#[derive(Serialize, Deserialize)]
pub struct WireRequest {
    pub body: Request,
}
impl WireRequest {
    pub fn new(request: Request) -> WireRequest {
        WireRequest { body: request }
    }
}

#[derive(Serialize, Deserialize)]
pub struct WireResponse {
    pub body: Response,
}
impl WireResponse {
    pub fn new(response: Response) -> WireResponse {
        WireResponse { body: response }
    }
}

type SinkTableLock = Arc<Mutex<SinkTable>>;
type NetworkFrameRead = FramedRead<OwnedReadHalf, LengthDelimitedCodec>;
type NetworkFrameWrite = FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>;

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
    request_tx: RequestTx,
    response_rx: ResponseRx,
    sink_table_lock: SinkTableLock,
}
impl Server {
    pub fn new(port: u16, request_tx: RequestTx, response_rx: ResponseRx) -> Server {
        Server {
            port,
            request_tx,
            response_rx,
            sink_table_lock: Arc::new(Mutex::new(SinkTable::new())),
        }
    }

    pub async fn listen(self) {
        let Server {
            port,
            request_tx,
            response_rx,
            sink_table_lock: lock,
        } = self;

        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(address).await.expect("bind tcp");
        println!("coalescentdb: server started at {}", address);

        tokio::select! {
            _ = Server::accept_connections(listener, lock.clone(), request_tx) => {}
            _ = Server::process_responses(lock.clone(), response_rx) => {}
        };
    }

    async fn accept_connections(
        listener: TcpListener,
        sink_table_lock: SinkTableLock,
        request_tx: RequestTx,
    ) {
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    println!("accepted connection from {}", addr);

                    let (read, write) = stream.into_split();
                    let stream = NetworkFrameRead::new(read, LengthDelimitedCodec::new());
                    let sink = NetworkFrameWrite::new(write, LengthDelimitedCodec::new());

                    let request_tx = request_tx.clone();
                    let sink_id;
                    {
                        sink_id = sink_table_lock.lock().unwrap().insert(sink);
                    }

                    tokio::spawn(Server::handle_connection(
                        stream,
                        sink_id,
                        sink_table_lock.clone(),
                        request_tx,
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
        request_tx: RequestTx,
    ) {
        while let Some(frame) = stream.next().await {
            let bytes = match frame {
                Ok(b) => b,
                Err(e) => {
                    println!("tcp frame decode failure: {:?}", e);
                    continue;
                }
            };
            let wire_request: WireRequest = match rmp_serde::from_slice(bytes.as_ref()) {
                Ok(r) => r,
                Err(e) => {
                    println!("msgpack deserialization fail: {:?}", e);
                    continue;
                }
            };
            let request = RequestDesc {
                sink_id,
                body: wire_request.body,
            };
            if let Err(_) = request_tx.send(request) {
                // channel is closed, close the connection
                break;
            }
        }

        // connection is closed, drop the sink
        sink_table_lock.lock().unwrap().remove(sink_id);
        println!("coalescentdb: closing connection @ id: {}", sink_id);
    }

    async fn process_responses(sink_table_lock: SinkTableLock, mut response_rx: ResponseRx) {
        while let Some(response) = response_rx.recv().await {
            let wire_response = WireResponse {
                body: response.body,
            };
            let bytes = match rmp_serde::to_vec(&wire_response) {
                Ok(b) => b,
                Err(e) => {
                    println!("msgpack serialization fail: {:?}", e);
                    continue;
                }
            };

            let sink_id = response.sink_id;
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
    }
}
