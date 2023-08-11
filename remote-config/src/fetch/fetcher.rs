use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use base64::Engine;
use hyper::http::uri::{PathAndQuery, Scheme};
use hyper::{Body, Client, StatusCode};
use sha2::{Digest, Sha256, Sha512};
use tracing::{debug, trace, warn};
use datadog_trace_protobuf::remoteconfig::{ClientGetConfigsRequest, ClientGetConfigsResponse, ClientState, ClientTracer, ConfigState, TargetFileHash, TargetFileMeta};
use ddcommon::{connector, Endpoint};
use crate::{RemoteConfigPath, Target};
use crate::targets::TargetsList;

const PROD_INTAKE_SUBDOMAIN: &str = "config";

/// Manages files.
/// Presents store() and update() operations.
/// It is recommended to minimize the overhead of these operations as they
pub trait FileStorage {
    type StoredFile;

    /// A new, currently unknown file was received.
    fn store(&self, version: u64, path: RemoteConfigPath, contents: Vec<u8>) -> anyhow::Result<Arc<Self::StoredFile>>;

    /// A file at a given path was updated (new contents).
    fn update(&self, file: &Arc<Self::StoredFile>, version: u64, contents: Vec<u8>) -> anyhow::Result<()>;
}

/// Fundamental configuration of the RC client, which always must be set.
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct ConfigInvariants {
    pub language: String,
    pub tracer_version: String,
    pub endpoint: Endpoint,
}

struct StoredTargetFile<S> {
    hash: String,
    handle: Arc<S>,
    state: ConfigState,
    meta: TargetFileMeta,
}

pub struct ConfigFetcherState<S> {
    target_files_by_path: Mutex<HashMap<String, StoredTargetFile<S>>>,
    pub invariants: ConfigInvariants,
    endpoint: Endpoint,
    pub expire_unused_files: bool,
}

pub struct ConfigFetcherFilesLock<'a, S> {
    inner: MutexGuard<'a, HashMap<String, StoredTargetFile<S>>>,
}

impl<'a, S> ConfigFetcherFilesLock<'a, S> {
    pub fn expire_file(&mut self, path: &RemoteConfigPath) {
        self.inner.remove(&path.to_string());
    }
}

impl<S> ConfigFetcherState<S> {
    pub fn new(invariants: ConfigInvariants) -> Self {
        ConfigFetcherState {
            target_files_by_path: Default::default(),
            endpoint: get_product_endpoint(PROD_INTAKE_SUBDOMAIN, &invariants.endpoint),
            invariants,
            expire_unused_files: true,
        }
    }

    /// To remove unused remote files manually. Must not be called when auto expiration is active.
    /// Note: careful attention must be paid when using this API in order to not deadlock:
    /// - This files_lock() must always be called prior to locking any data structure locked within
    ///   FileStorage::store().
    /// - Also, files_lock() must not be called from within FileStorage::store().
    pub fn files_lock(&self) -> ConfigFetcherFilesLock<S> {
        assert!(!self.expire_unused_files);
        ConfigFetcherFilesLock {
            inner: self.target_files_by_path.lock().unwrap()
        }
    }
}

pub struct ConfigFetcher<S: FileStorage> {
    pub file_storage: S,
    state: Arc<ConfigFetcherState<S::StoredFile>>,
    timeout: AtomicU32,
    /// Collected interval. May be zero if not provided by the remote config server or fetched yet.
    pub interval: AtomicU64,
}

#[derive(Default)]
pub struct OpaqueState {
    client_state: Vec<u8>,
}

impl<S: FileStorage> ConfigFetcher<S> {
    pub fn new(file_storage: S, state: Arc<ConfigFetcherState<S::StoredFile>>) -> Self {
        ConfigFetcher {
            file_storage,
            state,
            timeout: AtomicU32::new(5000),
            interval: AtomicU64::new(0),
        }
    }

    /// Quite generic fetching implementation:
    ///  - runs a request against the Remote Config Server,
    ///  - validates the data,
    ///  - removes unused files
    ///  - checks if the files are already known,
    ///  - stores new files,
    ///  - returns all currently active files.
    /// It also makes sure that old files are dropped before new files are inserted.
    pub async fn fetch_once(
        &mut self,
        runtime_id: &str,
        target: Arc<Target>,
        config_id: &str,
        last_error: Option<String>,
        opaque_state: &mut OpaqueState,
    ) -> anyhow::Result<Vec<Arc<S::StoredFile>>> {
        let Target { service, env, app_version } = (*target).clone();

        let mut cached_target_files = vec![];
        let mut config_states = vec![];

        for StoredTargetFile { state, meta, .. } in self.state.target_files_by_path.lock().unwrap().values() {
            config_states.push(state.clone());
            cached_target_files.push(meta.clone());
        }

        let config_req = ClientGetConfigsRequest {
            client: Some(datadog_trace_protobuf::remoteconfig::Client {
                state: Some(ClientState {
                    root_version: 1,
                    targets_version: 0,
                    config_states,
                    has_error: last_error.is_some(),
                    error: last_error.unwrap_or_default(),
                    backend_client_state: std::mem::take(&mut opaque_state.client_state),
                }),
                id: config_id.into(),
                // TODO maybe not hardcode requested products?
                products: vec!["APM_TRACING".to_string(), "LIVE_DEBUGGING".to_string()],
                is_tracer: true,
                client_tracer: Some(ClientTracer {
                    runtime_id: runtime_id.to_string(),
                    language: self.state.invariants.language.to_string(),
                    tracer_version: self.state.invariants.tracer_version.clone(),
                    service,
                    extra_services: vec![],
                    env,
                    app_version,
                    tags: vec![],
                }),
                is_agent: false,
                client_agent: None,
                last_seen: 0,
                capabilities: vec![],
            }),
            cached_target_files,
        };
        let json = serde_json::to_string(&config_req)?;

        // TODO: directly talking to datadog endpoint (once signatures are validated)
        let req = self.state.endpoint
            .into_request_builder(concat!("Sidecar/", env!("CARGO_PKG_VERSION")))?;
        let response = Client::builder()
            .build(connector::Connector::default())
            .request(req.body(Body::from(json))?)
            .await
            .map_err(|e| anyhow::Error::msg(e).context(format!("Url: {:?}", self.state.endpoint)))?;
        let status = response.status();
        let body_bytes = hyper::body::to_bytes(response.into_body()).await?;
        if status != StatusCode::OK {
            let response_body =
                String::from_utf8(body_bytes.to_vec()).unwrap_or_default();
            anyhow::bail!("Server did not accept traces: {response_body}");
        }

        // Agent remote config not active or broken or similar
        if body_bytes.len() <= 3 {
            trace!("Requested remote config, but not active; received: {}", String::from_utf8_lossy(body_bytes.as_ref()));
            return Ok(vec![]);
        }

        let response: ClientGetConfigsResponse =
            serde_json::from_str(&String::from_utf8_lossy(body_bytes.as_ref()))?;

        let decoded_targets = base64::engine::general_purpose::STANDARD.decode(response.targets.as_slice())?;
        let targets_list = TargetsList::try_parse(decoded_targets.as_slice()).map_err(|e| anyhow::Error::msg(e).context(format!("Decoded targets reply: {}", String::from_utf8_lossy(decoded_targets.as_slice()))))?;
        // TODO: eventually also verify the targets_list.signatures for FIPS compliance.

        opaque_state.client_state = targets_list.signed.custom.opaque_backend_state.as_bytes().to_vec();
        if let Some(interval) = targets_list.signed.custom.agent_refresh_interval {
            self.interval.store(interval, Ordering::Relaxed);
        }

        trace!("Received remote config of length {}, containing {:?} paths for target {:?}", body_bytes.len(), targets_list.signed.targets.keys().collect::<Vec<_>>(), target);

        let incoming_files: HashMap<_, _> = response.target_files.iter().map(|f| (f.path.as_str(), f.raw.as_slice())).collect();

        // This lock must be held continuously at least between the existence check
        // (target_files.get()) and the insertion later on. Makes more sense to just hold it continuously
        let mut target_files = self.state.target_files_by_path.lock().unwrap();

        if self.state.expire_unused_files {
            target_files.retain(|k, _| {
                targets_list.signed.targets.contains_key(k.as_str())
            });
        }

        for (path, target_file) in targets_list.signed.targets {
            fn hash_sha256(v: &[u8]) -> String { format!("{:x}", Sha256::digest(v)) }
            fn hash_sha512(v: &[u8]) -> String { format!("{:x}", Sha512::digest(v)) }
            let (hasher, hash) = if let Some(sha256) = target_file.hashes.get("sha256") {
                (hash_sha256 as fn(&[u8]) -> String, *sha256)
            } else if let Some(sha512) = target_file.hashes.get("sha512") {
                (hash_sha512 as fn(&[u8]) -> String, *sha512)
            } else {
                warn!("Found a target file without hashes at path {path}");
                continue;
            };
            let handle = if let Some(StoredTargetFile { hash: old_hash, handle, .. }) = target_files.get(path) {
                if old_hash == hash {
                    continue;
                }
                Some(handle.clone())
            } else {
                None
            };
            if let Some(raw_file) = incoming_files.get(path) {
                if let Ok(decoded) = base64::engine::general_purpose::STANDARD
                    .decode(raw_file)
                {
                    let computed_hash = hasher(decoded.as_slice());
                    if hash != computed_hash {
                        warn!("Computed hash of file {computed_hash} did not match remote config targets file hash {hash} for path {path}: file: {}", String::from_utf8_lossy(decoded.as_slice()));
                        continue;
                    }

                    match RemoteConfigPath::try_parse(path) {
                        Ok(parsed_path) => if let Some(version) = target_file.try_parse_version() {
                            debug!("Fetched new remote config file at path {path} targeting {target:?}");

                            target_files.insert(path.to_string(), StoredTargetFile {
                                hash: computed_hash,
                                state: ConfigState {
                                    id: parsed_path.config_id.to_string(),
                                    version,
                                    product: parsed_path.product.to_string(),
                                    apply_state: 0,
                                    apply_error: "".to_string(),
                                },
                                meta: TargetFileMeta {
                                    path: path.to_string(),
                                    length: decoded.len() as i64,
                                    hashes: target_file.hashes.iter().map(|(algorithm, hash)| TargetFileHash {
                                        algorithm: algorithm.to_string(),
                                        hash: hash.to_string(),
                                    }).collect(),
                                },
                                handle: if let Some(handle) = handle {
                                    self.file_storage.update(&handle, version, decoded)?;
                                    handle
                                } else {
                                    self.file_storage.store(version, parsed_path, decoded)?
                                },
                            });
                        } else {
                            warn!("Failed parsing version from remote config path {path}");
                        },
                        Err(e) => {
                            warn!("Failed parsing remote config path: {path} - {e:?}");
                        }
                    }
                } else {
                    warn!("Failed base64 decoding config for path {path}: {}", String::from_utf8_lossy(raw_file))
                }
            } else {
                warn!("Found changed config data for path {path}, but no file; existing files: {:?}", incoming_files.keys().collect::<Vec<_>>())
            }
        }

        let mut configs = Vec::with_capacity(response.client_configs.len());
        for config in response.client_configs.iter() {
            if let Some(StoredTargetFile { handle, .. }) = target_files.get(config) {
                configs.push(handle.clone());
            }
        }

        Ok(configs)
    }
}

fn get_product_endpoint(subdomain: &str, endpoint: &Endpoint) -> Endpoint {
    let mut parts = endpoint.url.clone().into_parts();
    if endpoint.api_key.is_some() {
        if parts.scheme.is_none() {
            parts.scheme = Some(Scheme::HTTPS);
            parts.authority = Some(
                format!("{}.{}", subdomain, parts.authority.unwrap())
                    .parse()
                    .unwrap(),
            );
        }
        parts.path_and_query = Some(PathAndQuery::from_static("/api/v0.1/configurations"));
    } else {
        parts.path_and_query = Some(PathAndQuery::from_static("/v0.7/config"));
    }
    Endpoint {
        url: hyper::Uri::from_parts(parts).unwrap(),
        api_key: endpoint.api_key.clone(),
    }
}
