use crate::broker_service::{ProtoBroker, ProtoAckedMsgs};
use crate::topic::Topic;
use crate::msg::Msg;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use prost::Message;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Write};
use tonic::codegen::Body;
use tracing::{info};

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Topic '{0}' already exists")]
    TopicAlreadyExists(String),
    #[error("Topic '{0}' not found")]
    TopicNotFound(String),
    #[error("Message '{0}' not found")]
    MessageNotFound(String),
}

#[derive(Debug, Clone, Default)]
pub struct Broker {
    pub topics: HashMap<String, Topic>,
    pub acked_msgs: HashMap<String, HashSet<String>>
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
            acked_msgs: HashMap::new(),
        }
    }

    pub async fn create_topic(&mut self, name: &str) -> Result<(), BrokerError> {
        match self.topics.entry(name.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(Topic::new(name));
                Ok(())
            }
            Entry::Occupied(_) => Err(BrokerError::TopicAlreadyExists(name.to_string())),
        }
    }

    pub async fn subscribe(&mut self, topic_name: &str, client_id: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                topic.subscribers.insert(client_id.to_string());
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub async fn unsubscribe(&mut self, topic_name: &str, client_id: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                topic.subscribers.remove(client_id);
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub async fn post(&mut self, topic_name: &str, payload: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                let msg = Msg::new(payload);
                let msg_id = msg.id.clone();
                self.acked_msgs.insert(msg_id, HashSet::new());
                topic.msgs.push(msg);
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub async fn ack(&mut self, msg_id: &str, client_id: &str) -> Result<(), BrokerError> {
        match self.acked_msgs.get_mut(msg_id) {
            None => Err(BrokerError::MessageNotFound(msg_id.to_string())),
            Some(msg) => {
                msg.insert(client_id.to_string());
                Ok(())
            },
        }
    }

    fn from_proto(proto: ProtoBroker) -> Self {
        let topics = proto
            .topics
            .into_iter()
            .map(|(key, topic)| (key, Topic::from_proto(topic)))
            .collect();

        let acked_msgs = proto
            .acked_msgs
            .into_iter()
            .map(|(key, acked_msgs)| {
                let messages: HashSet<String> = acked_msgs.messages.into_iter().collect();
                (key, messages)
            })
            .collect();

        Broker { topics, acked_msgs }
}

    pub fn to_proto(&self) -> ProtoBroker {
        let topics = self
            .topics
            .iter()
            .map(|(key, topic)| {
                (key.clone(), topic.to_proto())
            })
            .collect();

        let acked_msgs = self
            .acked_msgs
            .iter()
            .map(|(key, client_ids)| {
                let client_ids_vector: Vec<String> = client_ids.iter().cloned().collect();
                let proto_acked_msgs = ProtoAckedMsgs {
                    messages: client_ids_vector,
                };
                (key.clone(), proto_acked_msgs)
            })
            .collect();

        ProtoBroker { topics, acked_msgs }
    }


    pub async fn save_to_file(&self, path: &str) -> Result<(), std::io::Error> {
        let proto = self.to_proto();
        let mut buf = Vec::new();
        proto.encode(&mut buf).unwrap();
        let mut file = File::create(path)?;
        file.write_all(&buf)?;
        Ok(())
    }

    pub async fn load_from_file(path: &str) -> Result<Self, std::io::Error> {
        let mut file = File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        match ProtoBroker::decode(&buf[..]) {
            Ok(proto) => { Ok(Self::from_proto(proto)) }
            Err(_) => { Err(Error::new(ErrorKind::InvalidData, "broker state data file corrupted")) }
        }
    }

    pub async fn fetch(&self, client_id: &str) -> Vec<&Msg> {
        let subscribed_topics = self.topics.iter()
            .filter(|t| t.1.subscribers.contains(&client_id.to_string()));

        let all_msgs: Vec<&Msg> = subscribed_topics
            .flat_map(|(_key, topic)| topic.msgs.iter())
            .collect();

        let new_msgs = all_msgs
            .into_iter()
            .filter(|msg| {
                match self.acked_msgs.get(&msg.id) {
                    Some (acked_clients) => !acked_clients.contains(client_id),
                    None => true,
                }
            })
            .collect();

        new_msgs
    }
}