use crate::broker_service::ProtoTopic;

use std::collections::HashSet;
use crate::msg::Msg;


#[derive(Debug, Clone)]
pub struct Topic {
    pub name: String,
    pub subscribers: HashSet<String>,
    pub msgs: Vec<Msg>,
}

impl Topic {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            subscribers: HashSet::new(),
            msgs: Vec::new(),
        }
    }

    pub fn from_proto(proto: ProtoTopic) -> Self {
        Self {
            name: proto.name,
            subscribers: proto.subscribers.into_iter().collect(),
            msgs: proto.msgs.into_iter().map(Msg::from_proto).collect(),
        }
    }

    pub fn to_proto(&self) -> ProtoTopic {
        ProtoTopic {
            name: self.name.clone(),
            subscribers: self.subscribers.iter().cloned().collect(),
            msgs: self.msgs.iter().map(Msg::to_proto).collect(),
        }
    }
}