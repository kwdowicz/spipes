mod broker_service {
    tonic::include_proto!("broker_service");
}

use crate::broker_service::ProtoBroker;
use crate::topic::Topic;
use crate::msg::Msg;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use thiserror::Error;
use prost::Message;
use std::fs::File;
use std::io::{Read, Write};

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Topic '{0}' already exists")]
    TopicAlreadyExists(String),
    #[error("Topic '{0}' not found")]
    TopicNotFound(String),
}

#[derive(Debug, Clone, Default)]
pub struct Broker {
    pub topics: HashMap<String, Topic>,
}

impl Broker {
    pub fn new() -> Self {
        Self {
            topics: HashMap::new(),
        }
    }

    pub fn create_topic(&mut self, name: &str) -> Result<(), BrokerError> {
        match self.topics.entry(name.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(Topic::new(name));
                Ok(())
            }
            Entry::Occupied(_) => Err(BrokerError::TopicAlreadyExists(name.to_string())),
        }
    }

    pub fn subscribe(&mut self, topic_name: &str, client_id: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                topic.subscribers.insert(client_id.to_string());
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub fn unsubscribe(&mut self, topic_name: &str, client_id: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                topic.subscribers.remove(client_id);
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub fn post(&mut self, topic_name: &str, payload: &str) -> Result<(), BrokerError> {
        match self.topics.get_mut(topic_name) {
            Some(topic) => {
                topic.msgs.push(Msg { payload: payload.to_string() });
                Ok(())
            }
            None => Err(BrokerError::TopicNotFound(topic_name.to_string())),
        }
    }

    pub fn from_proto(proto: ProtoBroker) -> Self {
        let topics = proto
            .topics
            .into_iter()
            .map(|t| (t.name.clone(), Topic::from_proto(t)))
            .collect();
        Self { topics }
    }

    pub fn to_proto(&self) -> ProtoBroker {
        let topics = self
            .topics
            .values()
            .map(Topic::to_proto)
            .collect();
        ProtoBroker { topics }
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
        let proto = ProtoBroker::decode(&buf[..]).unwrap();
        Ok(Self::from_proto(proto))
    }
}