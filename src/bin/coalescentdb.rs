use coalescent_database::{engine::Engine, networking::server};
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    let server_port = 27800;

    let (engine_tx, engine_rx) = mpsc::unbounded_channel();
    let (server_tx, server_rx) = mpsc::unbounded_channel();

    // let server = Server::new(server_port, server_tx, engine_rx);
    let engine = Engine::new(engine_tx, server_rx);

    tokio::select! {
        _ = server::listen(server_port, server_tx, engine_rx) => {}
        _ = engine.run() => {}
    }
}
