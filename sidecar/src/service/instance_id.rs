use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Hash, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct InstanceId {
    pub session_id: String,
    pub runtime_id: String,
}

impl InstanceId {
    pub fn new<T>(session_id: T, runtime_id: T) -> Self
    where
        T: Into<String>,
    {
        InstanceId {
            session_id: session_id.into(),
            runtime_id: runtime_id.into(),
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_id_new() {
        let session_id = "test_session";
        let runtime_id = "test_runtime";

        let instance_id = InstanceId::new(session_id, runtime_id);

        assert_eq!(instance_id.session_id, session_id);
        assert_eq!(instance_id.runtime_id, runtime_id);
    }
}
