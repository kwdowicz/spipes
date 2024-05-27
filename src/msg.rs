use crate::broker_service::ProtoMsg;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Msg {
    pub payload: String,
    pub id: String,
}

impl Msg {
    pub fn new(payload: &str) -> Self {
        Self {
            payload: payload.to_string(),
            id: Uuid::new_v4().to_string(),
        }
    }

    pub fn from_proto(proto: ProtoMsg) -> Self {
        Self {
            payload: proto.payload,
            id: proto.id,
        }
    }

    pub fn to_proto(&self) -> ProtoMsg {
        ProtoMsg {
            payload: self.payload.clone(),
            id: self.id.clone(),
        }
    }
}
