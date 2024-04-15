use super::InstanceId;

pub trait RequestIdentification {
    fn extract_identifier(&self) -> RequestIdentifier;
}

pub enum RequestIdentifier {
    InstanceId(InstanceId),
    SessionId(String),
    None,
}
