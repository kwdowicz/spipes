use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use thiserror::Error;

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
}

#[derive(Debug, Clone)]
pub struct Topic {
    name: String,
    subscribers: HashSet<String>,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            subscribers: HashSet::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let mut broker = Broker::new();
        assert!(broker.create_topic("topic1").is_ok());
        assert!(broker.topics.contains_key("topic1"));
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        let result = broker.create_topic("topic1");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Topic 'topic1' already exists"
        );
    }

    #[test]
    fn test_subscribe_to_existing_topic() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        assert!(broker.subscribe("topic1", "client1").is_ok());
        assert!(broker
            .topics
            .get("topic1")
            .unwrap()
            .subscribers
            .contains("client1"));
    }

    #[test]
    fn test_subscribe_to_existing_topic_twice() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        assert!(broker.subscribe("topic1", "client1").is_ok());
        assert!(broker
            .topics
            .get("topic1")
            .unwrap()
            .subscribers
            .contains("client1"));
        assert!(broker.subscribe("topic1", "client1").is_ok());
        assert_eq!(broker.topics.get("topic1").unwrap().subscribers.len(), 1);
    }

    #[test]
    fn test_unsubscribe_to_existing_topic_subscribed() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        assert!(broker.subscribe("topic1", "client1").is_ok());
        assert!(broker
            .topics
            .get("topic1")
            .unwrap()
            .subscribers
            .contains("client1"));
        assert!(broker.unsubscribe("topic1", "client1").is_ok());
        assert_eq!(broker.topics.get("topic1").unwrap().subscribers.len(), 0);
    }

    #[test]
    fn test_unsubscribe_to_existing_topic_not_subscribed() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        assert!(broker.unsubscribe("topic1", "client1").is_ok());
    }

    #[test]
    fn test_unsubscribe_to_non_existing_topic_not_subscribed() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        assert!(broker.unsubscribe("topic2", "client1").is_err());
    }

    #[test]
    fn test_subscribe_to_non_existent_topic() {
        let mut broker = Broker::new();
        let result = broker.subscribe("non_existent_topic", "client1");
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Topic 'non_existent_topic' not found"
        );
    }

    #[test]
    fn test_multiple_subscribers() {
        let mut broker = Broker::new();
        broker.create_topic("topic1").unwrap();
        broker.subscribe("topic1", "client1").unwrap();
        broker.subscribe("topic1", "client2").unwrap();
        let subscribers = &broker.topics.get("topic1").unwrap().subscribers;
        assert!(subscribers.contains("client1"));
        assert!(subscribers.contains("client2"));
    }
}
