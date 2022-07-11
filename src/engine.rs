use crate::networking::server;

use bytes::Bytes;
use home::home_dir;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Deserialize, Serialize)]
pub enum Request {
    Store,
    Get,
    Drop,
    Link,
    Query,
}

#[derive(Deserialize, Serialize)]
pub enum Response {
    Value,
    Ok,
    Error,
}

pub enum Message {
    Request { sink_id: usize, request: Request },
}
pub(crate) type MessageTx = UnboundedSender<Message>;
pub(crate) type MessageRx = UnboundedReceiver<Message>;

pub struct Engine {
    db: DB,
    server_message_tx: server::MessageTx,
    message_rx: MessageRx,
}
impl Engine {
    pub fn new(server_message_tx: server::MessageTx, message_rx: MessageRx) -> Engine {
        Engine {
            db: Engine::open_db(),
            server_message_tx,
            message_rx,
        }
    }
    pub async fn run(mut self) {
        println!("coalescentdb: engine running");

        while let Some(message) = self.message_rx.recv().await {
            match message {
                Message::Request { sink_id, request } => {
                    let response = match request {
                        Request::Store => Response::Ok,
                        Request::Get => Response::Value,
                        Request::Drop => Response::Ok,
                        Request::Link => Response::Ok,
                        Request::Query => Response::Value,
                    };
                    let message = server::Message::Response { sink_id, response };

                    if let Err(_) = self.server_message_tx.send(message) {
                        // channel is closed, close the connection
                        break;
                    }
                }
            }
        }
    }

    fn open_db() -> DB {
        let mut path = home_dir().unwrap();
        path.push(".coalescentdb");
        match DB::open_default(path) {
            Ok(db) => db,
            Err(e) => {
                panic!("couldn't open database: {:?}", e)
            }
        }
    }

    fn insert(db: DB, data: Bytes) {
        if let Err(e) = db.put(b"my key", b"my value") {
            println!("rocksdb error: {}", e)
        }
    }
    fn read(db: DB, key: Bytes) {
        match db.get(key) {
            Ok(Some(value)) => {
                println!("retrieved value {}", String::from_utf8(value).unwrap())
            }
            Ok(None) => println!("value not found"),
            Err(e) => println!("rocksdb error:  {}", e),
        }
    }
    fn delete(db: DB, key: Bytes) {
        if let Err(e) = db.delete(key) {
            println!("rocksdb error: {}", e);
        }
    }
}
