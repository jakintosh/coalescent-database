use bytes::Bytes;
use home::home_dir;
use rocksdb::DB;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Deserialize, Serialize)]
pub enum Request {
    Read,
    Insert,
    Delete,
}
pub struct RequestDesc {
    pub sink_id: usize,
    pub body: Request,
}
pub(crate) type RequestTx = UnboundedSender<RequestDesc>;
pub(crate) type RequestRx = UnboundedReceiver<RequestDesc>;

#[derive(Deserialize, Serialize)]
pub enum Response {
    Value,
    Ok,
    Error,
}
pub struct ResponseDesc {
    pub sink_id: usize,
    pub body: Response,
}
pub(crate) type ResponseTx = UnboundedSender<ResponseDesc>;
pub(crate) type ResponseRx = UnboundedReceiver<ResponseDesc>;

pub struct Engine {
    db: DB,
    request_rx: RequestRx,
    response_tx: ResponseTx,
}
impl Engine {
    pub fn new(request_rx: RequestRx, response_tx: ResponseTx) -> Engine {
        Engine {
            db: Engine::open_db(),
            request_rx,
            response_tx,
        }
    }
    pub async fn run(mut self) {
        println!("coalescentdb: engine running");
        while let Some(request) = self.request_rx.recv().await {
            println!(
                "coalescentdb: received request from conn_id: {}",
                request.sink_id
            );
            let body = match request.body {
                Request::Read => Response::Value,
                Request::Insert => Response::Ok,
                Request::Delete => Response::Ok,
            };
            let response = ResponseDesc {
                sink_id: request.sink_id,
                body,
            };
            println!(
                "coalescentdb: sending response to conn_id: {}",
                request.sink_id
            );
            if let Err(_) = self.response_tx.send(response) {
                // channel is closed, close the connection
                break;
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
