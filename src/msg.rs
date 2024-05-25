mod broker_service {
    tonic::include_proto!("broker_service");
}
use crate::broker_service::ProtoMsg;

#[derive(Debug, Clone)]
pub struct Msg {
    pub payload: String,
}

impl Msg {
    pub fn from_proto(proto: ProtoMsg) -> Self {
        Self {
            payload: proto.payload,
        }
    }

    pub fn to_proto(&self) -> ProtoMsg {
        ProtoMsg {
            payload: self.payload.clone(),
        }
    }
}
