mod broker;

use broker::Broker;
use broker_service::broker_service_server::{BrokerService, BrokerServiceServer};
use broker_service::{
    CreateTopicRequest, CreateTopicResponse, SubscribeRequest, SubscribeResponse,
};
use std::sync::{Arc, Mutex};
use tonic::{transport::Server, Request, Response, Status};

mod broker_service {
    tonic::include_proto!("broker_service");
}

#[derive(Debug, Default)]
pub struct BrokerServiceImpl {
    broker: Arc<Mutex<Broker>>,
}

#[tonic::async_trait]
impl BrokerService for BrokerServiceImpl {
    async fn create_topic(
        &self,
        request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().unwrap();

        match broker.create_topic(&req.name) {
            Ok(_) => Ok(Response::new(CreateTopicResponse {
                message: format!("Topic '{}' created", req.name),
            })),
            Err(e) => Err(Status::already_exists(e.to_string())),
        }
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().unwrap();

        match broker.subscribe(&req.topic_name, &req.client_id) {
            Ok(_) => Ok(Response::new(SubscribeResponse {
                message: format!(
                    "Client '{}' subscribed to '{}'",
                    req.client_id, req.topic_name
                ),
            })),
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }
}

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:5005".parse()?;
    let broker = Arc::new(Mutex::new(Broker::new()));
    let broker_service = BrokerServiceImpl { broker };

    println!("Listening on {}", addr);

    Server::builder()
        .add_service(BrokerServiceServer::new(broker_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    start_server().await
}