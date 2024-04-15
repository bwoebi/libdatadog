use serde::{Deserialize, Serialize};

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct RuntimeMetadata {
    pub language_name: String,
    pub language_version: String,
    pub tracer_version: String,
}

impl RuntimeMetadata {
    pub fn new<T>(language_name: T, language_version: T, tracer_version: T) -> Self
    where
        T: Into<String>,
    {
        Self {
            language_name: language_name.into(),
            language_version: language_version.into(),
            tracer_version: tracer_version.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_runtime_metadata_new() {
        let language_name = "Rust";
        let language_version = "1.55.0";
        let tracer_version = "0.1.0";

        let metadata = RuntimeMetadata::new(language_name, language_version, tracer_version);

        assert_eq!(metadata.language_name, language_name);
        assert_eq!(metadata.language_version, language_version);
        assert_eq!(metadata.tracer_version, tracer_version);
    }
}
