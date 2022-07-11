use crate::{
    engine,
    networking::{sink_table, NetworkFrameRead, NetworkFrameWrite, WireMessage},
};
use futures::StreamExt;
use std::net::SocketAddr;
use tokio::{
    net::TcpListener,
    sync::{
        mpsc::{self, UnboundedReceiver, UnboundedSender},
        oneshot,
    },
};
use tokio_util::codec::LengthDelimitedCodec;

pub enum Message {
    Response {
        sink_id: usize,
        response: engine::Response,
    },
}
pub type MessageTx = UnboundedSender<Message>;
pub type MessageRx = UnboundedReceiver<Message>;

pub async fn listen(port: u16, engine_message_tx: engine::MessageTx, server_message_rx: MessageRx) {
    let (sink_message_tx, sink_message_rx) = mpsc::unbounded_channel();

    let address = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = TcpListener::bind(address).await.expect("bind tcp");
    println!("coalescentdb: server started at {}", address);

    // kick off processes
    tokio::select! {
        _ = sink_table::run_sink_table(sink_message_rx) => {}
        _ = accept_connections(listener, engine_message_tx, sink_message_tx.clone()) => {}
        _ = poll_messages(server_message_rx, sink_message_tx) => {},
    };
}

async fn accept_connections(
    listener: TcpListener,
    engine_message_tx: engine::MessageTx,
    sink_message_tx: sink_table::MessageTx,
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
                if let Err(_) = sink_message_tx.send(sink_table::Message::Insert { sink, id_tx }) {
                    println!("sink table is closed, cannot accept any more connections, aborting");
                    return;
                }
                let sink_id = match id_rx.await {
                    Ok(id) => id,
                    Err(_) => {
                        println!("couldn't store connection sink");
                        continue;
                    }
                };
                let engine_message_tx = engine_message_tx.clone();
                tokio::spawn(poll_connection(
                    stream,
                    engine_message_tx,
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
    message_tx: engine::MessageTx,
    sink_id: usize,
    sink_message_tx: sink_table::MessageTx,
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
                match message_tx.send(engine::Message::Request { sink_id, request }) {
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
    sink_message_tx.send(sink_table::Message::Remove(sink_id));
    println!("coalescentdb: closing connection @ id: {}", sink_id);
}

async fn poll_messages(mut message_rx: MessageRx, sink_message_tx: sink_table::MessageTx) {
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

                sink_message_tx.send(sink_table::Message::Send {
                    id: sink_id,
                    bytes: bytes.into(),
                });
            }
        };
    }
}
