mod broker;
mod server;
mod utils;
mod topic;
mod msg;
mod broker_service;

use crate::server::start_server;
use tracing_subscriber::FmtSubscriber;
//use tracing_appender::rolling::{RollingFileAppender, Rotation};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    //let file_appender = RollingFileAppender::new(Rotation::HOURLY, "logs", "app.log");
    //let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);
    let subscriber = FmtSubscriber::builder()
        .with_max_level(tracing::Level::INFO)
        //.with_writer(non_blocking)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    start_server().await
}
