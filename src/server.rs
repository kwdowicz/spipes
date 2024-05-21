use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};
use tonic::transport::Server;
use crate::broker::Broker;
use crate::broker_service::broker_service_server::{BrokerService, BrokerServiceServer};
use crate::broker_service::{CreateTopicRequest, CreateTopicResponse, SubscribeRequest, SubscribeResponse};

const BROKER_STATE_FILE: &str = "broker_state.bin";
const SERVER_ADDR: &str = "127.0.0.1:5005";

#[derive(Debug, Default)]
pub struct BrokerServiceImpl {
    broker: Arc<Mutex<Broker>>,
}

#[tonic::async_trait]
impl BrokerService for BrokerServiceImpl {
    async fn create_topic(&self,request: Request<CreateTopicRequest>,) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().unwrap();

        match broker.create_topic(&req.name) {
            Ok(_) => {
                broker.save_to_file(BROKER_STATE_FILE).unwrap();
                Ok(Response::new(CreateTopicResponse {
                    message: format!("Topic '{}' created", req.name),
                }))
            }
            Err(e) => Err(Status::already_exists(e.to_string())),
        }
    }

    async fn subscribe(&self,request: Request<SubscribeRequest>,) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().unwrap();

        match broker.subscribe(&req.topic_name, &req.client_id) {
            Ok(_) => {
                broker.save_to_file(BROKER_STATE_FILE).unwrap();
                Ok(Response::new(SubscribeResponse {
                    message: format!(
                        "Client '{}' subscribed to '{}'",
                        req.client_id, req.topic_name
                    ),
                }))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }
}

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SERVER_ADDR.parse()?;
    let broker = match Broker::load_from_file(BROKER_STATE_FILE) {
        Ok(broker) => {
            println!("Broker: {:#?}", broker.clone());
            Arc::new(Mutex::new(broker))
        },
        Err(_) => Arc::new(Mutex::new(Broker::new())),
    };
    let broker_service = BrokerServiceImpl { broker };

    println!("Listening on {}", addr);

    Server::builder()
        .add_service(BrokerServiceServer::new(broker_service))
        .serve(addr)
        .await?;

    Ok(())
}