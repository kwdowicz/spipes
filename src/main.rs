mod broker;
mod server;
mod utils;

use crate::server::start_server;

mod broker_service {
    tonic::include_proto!("broker_service");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_server().await
}
