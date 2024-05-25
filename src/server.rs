use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Code, Request, Response, Status};
use tonic::transport::Server;
use crate::broker::Broker;
use crate::broker_service::{CreateTopicRequest, CreateTopicResponse, SubscribeRequest, SubscribeResponse, UnsubscribeRequest, UnsubscribeResponse, PostRequest, PostResponse, FetchRequest, FetchResponse};
use crate::broker_service::broker_service_server::{BrokerService, BrokerServiceServer};
use tracing::{info};

const BROKER_STATE_FILE: &str = "broker_state.bin";
const SERVER_ADDR: &str = "127.0.0.1:5005";

#[derive(Debug, Default)]
pub struct BrokerServiceImpl {
    broker: Arc<Mutex<Broker>>,
}

#[tonic::async_trait]
impl BrokerService for BrokerServiceImpl {
    async fn create_topic(&self, request: Request<CreateTopicRequest>) -> Result<Response<CreateTopicResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().await;

        match broker.create_topic(&req.name).await {
            Ok(_) => {
                broker.save_to_file(BROKER_STATE_FILE).await.unwrap();
                Ok(Response::new(CreateTopicResponse {
                    message: format!("Topic '{}' created", req.name),
                }))
            }
            Err(e) => Err(Status::already_exists(e.to_string())),
        }
    }

    async fn subscribe(&self, request: Request<SubscribeRequest>) -> Result<Response<SubscribeResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().await;

        match broker.subscribe(&req.topic_name, &req.client_id).await {
            Ok(_) => {
                info!("Subscription: {:?}", &req);
                broker.save_to_file(BROKER_STATE_FILE).await.unwrap();
                Ok(Response::new(SubscribeResponse {
                    message: format!("Client '{}' subscribed to '{}'", req.client_id, req.topic_name),
                }))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }

    async fn unsubscribe(&self, request: Request<UnsubscribeRequest>) -> Result<Response<UnsubscribeResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().await;

        match broker.unsubscribe(&req.topic_name, &req.client_id).await {
            Ok(_) => {
                info!("Unsubscription: {:?}", &req);
                broker.save_to_file(BROKER_STATE_FILE).await.unwrap();
                Ok(Response::new(UnsubscribeResponse {
                    message: format!("Client '{}' unsubscribed from '{}'", req.client_id, req.topic_name),
                }))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }

    async fn post(&self, request: Request<PostRequest>) -> Result<Response<PostResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().await;

        match broker.post(&req.topic_name, &req.payload).await {
            Ok(_) => {
                info!("Post: {:?}", &req);
                broker.save_to_file(BROKER_STATE_FILE).await.unwrap();
                Ok(Response::new(PostResponse {
                    message: format!("Posted to '{}': '{}'", req.topic_name, req.payload),
                }))
            }
            Err(e) => Err(Status::not_found(e.to_string())),
        }
    }

    // So we are sending always a vector, even if all the request is wrong
    // think about it once more...
    async fn fetch(&self, request: Request<FetchRequest>) -> Result<Response<FetchResponse>, Status> {
        let req = request.into_inner();
        let mut broker = self.broker.lock().await;
        let msgs = broker.fetch(&req.client_id).await;
        let mut proto_msgs = vec![];
        for m in msgs {
            proto_msgs.push(m.to_proto());
        }
        Ok(Response::new(FetchResponse {
            msgs: proto_msgs,
        }))
    }
}

pub async fn start_server() -> Result<(), Box<dyn std::error::Error>> {
    let addr = SERVER_ADDR.parse()?;
    let broker = match Broker::load_from_file(BROKER_STATE_FILE).await {
        Ok(broker) => {
            println!("Broker: {:#?}", broker.clone());
            Arc::new(Mutex::new(broker))
        },
        Err(_) => Arc::new(Mutex::new(Broker::new())),
    };
    let broker_service = BrokerServiceImpl { broker };

    info!("Server started. Listening on {}", addr);

    Server::builder()
        .add_service(BrokerServiceServer::new(broker_service))
        .serve(addr)
        .await?;

    Ok(())
}
