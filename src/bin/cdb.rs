use bytes::{Bytes, BytesMut};
use clap::{Parser, Subcommand};
use coalescent_database::{
    engine::{Request, Response},
    server::{Client, WireRequest, WireResponse},
};
use futures::{SinkExt, StreamExt};

#[derive(Parser)]
#[clap(name = "cdb")]
#[clap(author = "@jakintosh")]
#[clap(version = "0.1.0")]
#[clap(about = "cli for the coalescent-database", long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Insert { data: String },
    Read { key: String },
    Delete { key: String },
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // connect to database
    let (mut stream, mut sink) = Client::connect(27800).await;

    // pack request and send
    let request = pack_request(args.command);
    sink.send(request).await.expect("failed to send request");

    // wait for response and unpack
    let response = unpack_response(
        stream
            .next()
            .await
            .expect("failed to read tcp stream")
            .expect("failed to decode frame"),
    );
    match response {
        Response::Value => println!("DB::RESPONSE::VALUE"),
        Response::Ok => println!("DB::RESPONSE::OK"),
        Response::Error => println!("DB::RESPONSE::ERROR"),
    }
}

fn pack_request(command: Commands) -> Bytes {
    let request = match command {
        Commands::Insert { .. } => Request::Insert,
        Commands::Read { .. } => Request::Read,
        Commands::Delete { .. } => Request::Delete,
    };
    let wire_request = WireRequest::new(request);
    rmp_serde::to_vec(&wire_request)
        .expect("msg pack serialize fail")
        .into()
}

fn unpack_response(bytes: BytesMut) -> Response {
    let response: WireResponse =
        rmp_serde::from_slice(bytes.as_ref()).expect("msgpack deserialization error");

    response.body
}
