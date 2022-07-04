use coalescent_database::{engine::Engine, server::Server};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let (request_tx, request_rx) = mpsc::unbounded_channel();
    let (response_tx, response_rx) = mpsc::unbounded_channel();
    let server_port = 27800;
    let server = Server::new(server_port, request_tx, response_rx);
    let engine = Engine::new(request_rx, response_tx);

    tokio::select! {
        _ = server.listen() => {}
        _ = engine.run() => {}
    }
}
