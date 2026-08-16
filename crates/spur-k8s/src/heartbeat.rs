// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;

use tokio::sync::RwLock;
use tonic::Status;
use tracing::{debug, warn};

use spur_proto::proto::slurm_controller_client::SlurmControllerClient;
use spur_proto::proto::{HeartbeatRequest, RegisterAgentRequest};

// Matches spurctld's check_node_health(90) timeout and spurd's 30 s interval.
const INTERVAL_SECS: u64 = 30;

/// Tracks the set of active K8s nodes and sends periodic `Heartbeat` RPCs
/// to spurctld on their behalf, mirroring what `spurd`'s `reporter::heartbeat_loop`
/// does for native-host nodes.
///
/// `node_watcher` holds an `Arc<HeartbeatManager>` and calls `track`/`untrack`
/// as nodes appear and disappear; the heartbeat task calls `run` under
/// `retry::run_with_retry`.
pub struct HeartbeatManager {
    registry: RwLock<HashMap<String, RegisterAgentRequest>>,
    controller_addr: String,
}

impl HeartbeatManager {
    pub fn new(controller_addr: String) -> Self {
        Self {
            registry: RwLock::new(HashMap::new()),
            controller_addr,
        }
    }

    /// Add or update a node in the tracked set.
    pub async fn track(&self, name: String, req: RegisterAgentRequest) {
        self.registry.write().await.insert(name, req);
    }

    /// Remove only the exact virtual worker that disappeared. A delayed Delete
    /// event for an old Kubernetes Node UID must not stop heartbeats for a
    /// same-name replacement.
    pub async fn untrack_exact(&self, name: &str, worker_incarnation: &str) -> bool {
        let mut registry = self.registry.write().await;
        if registry
            .get(name)
            .is_some_and(|request| request.worker_incarnation == worker_incarnation)
        {
            registry.remove(name);
            true
        } else {
            false
        }
    }

    /// Require a request to name the current incarnation of a specific virtual
    /// worker. This mirrors native spurd's incarnation gate and prevents a
    /// delayed launch/control RPC from a prior registration from mutating K8s.
    pub async fn require_incarnation(
        &self,
        name: &str,
        worker_incarnation: &str,
    ) -> Result<(), Status> {
        if worker_incarnation.is_empty() {
            return Err(Status::invalid_argument(
                "worker_incarnation must be nonempty",
            ));
        }
        let registry = self.registry.read().await;
        let Some(current) = registry.get(name) else {
            return Err(Status::failed_precondition(format!(
                "virtual worker {name} is not currently registered"
            )));
        };
        if current.worker_incarnation != worker_incarnation {
            return Err(Status::failed_precondition(format!(
                "virtual worker {name} incarnation is stale"
            )));
        }
        Ok(())
    }

    /// Resolve a current virtual worker by its opaque incarnation. Controller
    /// fan-out RPCs other than LaunchJob do not carry the target node name.
    pub async fn require_known_incarnation(
        &self,
        worker_incarnation: &str,
    ) -> Result<String, Status> {
        if worker_incarnation.is_empty() {
            return Err(Status::invalid_argument(
                "worker_incarnation must be nonempty",
            ));
        }
        self.registry
            .read()
            .await
            .iter()
            .find_map(|(name, request)| {
                (request.worker_incarnation == worker_incarnation).then(|| name.clone())
            })
            .ok_or_else(|| Status::failed_precondition("virtual worker incarnation is stale"))
    }

    /// Send `Heartbeat` RPCs to spurctld for every tracked node.
    pub async fn run(&self) -> anyhow::Result<()> {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(INTERVAL_SECS));
        loop {
            interval.tick().await;

            let registrations: Vec<(String, String)> = self
                .registry
                .read()
                .await
                .iter()
                .map(|(name, req)| (name.clone(), req.worker_incarnation.clone()))
                .collect();
            if registrations.is_empty() {
                continue;
            }

            match connect(&self.controller_addr).await {
                Ok(mut client) => {
                    for (name, worker_incarnation) in &registrations {
                        let req = HeartbeatRequest {
                            hostname: name.clone(),
                            cpu_load: 0,
                            free_memory_mb: 0,
                            running_jobs: vec![],
                            node_token: String::new(),
                            wg_pubkey: String::new(), // virtual agents are not on the mesh
                            k0s_status: None,         // virtual agents run no k0s unit
                            worker_incarnation: worker_incarnation.clone(),
                        };
                        match client.heartbeat(req).await {
                            Ok(_) => debug!(node = %name, "heartbeat sent"),
                            Err(e) => warn!(node = %name, error = %e, "heartbeat failed"),
                        }
                    }
                }
                Err(e) => warn!(error = %e, "heartbeat: failed to connect to spurctld"),
            }
        }
    }
}

async fn connect(addr: &str) -> anyhow::Result<SlurmControllerClient<tonic::transport::Channel>> {
    let url = if addr.starts_with("http") {
        addr.to_string()
    } else {
        format!("http://{}", addr)
    };
    Ok(SlurmControllerClient::connect(url)
        .await?
        .max_decoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_req(hostname: &str) -> RegisterAgentRequest {
        RegisterAgentRequest {
            hostname: hostname.into(),
            resources: None,
            version: "test".into(),
            address: "127.0.0.1".into(),
            port: 6818,
            wg_pubkey: String::new(),
            labels: std::collections::HashMap::new(),
            join_token: String::new(),
            worker_incarnation: format!("{hostname}-incarnation"),
        }
    }

    #[tokio::test]
    async fn test_new_registry_is_empty() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_track_adds_node() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;
        assert!(hb.registry.read().await.contains_key("node-1"));
    }

    #[tokio::test]
    async fn stale_incarnation_cannot_untrack_replacement() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        let mut replacement = make_req("node-1");
        replacement.worker_incarnation = "replacement".into();
        hb.track("node-1".into(), replacement).await;

        assert!(!hb.untrack_exact("node-1", "old").await);
        assert!(hb.registry.read().await.contains_key("node-1"));
        assert!(hb.untrack_exact("node-1", "replacement").await);
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_track_idempotent_updates_entry() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;

        let mut updated = make_req("node-1");
        updated.address = "10.0.0.1".into();
        hb.track("node-1".into(), updated).await;

        let guard = hb.registry.read().await;
        assert_eq!(guard.len(), 1);
        assert_eq!(guard["node-1"].address, "10.0.0.1");
    }

    #[tokio::test]
    async fn exact_incarnation_gate_rejects_stale_worker() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        let mut current = make_req("node-1");
        current.worker_incarnation = "current-incarnation".into();
        hb.track("node-1".into(), current).await;

        assert!(hb
            .require_incarnation("node-1", "current-incarnation")
            .await
            .is_ok());
        assert_eq!(
            hb.require_incarnation("node-1", "stale-incarnation")
                .await
                .expect_err("stale launch must be rejected")
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn test_multiple_nodes_tracked_independently() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        hb.track("node-3".into(), make_req("node-3")).await;
        assert_eq!(hb.registry.read().await.len(), 3);
    }

    #[tokio::test]
    async fn test_untrack_one_of_many_leaves_others() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        assert!(hb.untrack_exact("node-1", "node-1-incarnation").await);

        let guard = hb.registry.read().await;
        assert_eq!(guard.len(), 1);
        assert!(!guard.contains_key("node-1"));
        assert!(guard.contains_key("node-2"));
    }

    #[tokio::test]
    async fn test_track_after_untrack_re_adds_node() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;
        assert!(hb.untrack_exact("node-1", "node-1-incarnation").await);
        hb.track("node-1".into(), make_req("node-1")).await;

        assert_eq!(hb.registry.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_untrack_all_leaves_empty_registry() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        hb.track("node-1".into(), make_req("node-1")).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        assert!(hb.untrack_exact("node-1", "node-1-incarnation").await);
        assert!(hb.untrack_exact("node-2", "node-2-incarnation").await);
        assert!(hb.registry.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_register_req_preserved_for_reregistration() {
        let hb = HeartbeatManager::new("http://localhost:6817".into());
        let req = make_req("node-1");
        hb.track("node-1".into(), req.clone()).await;
        hb.track("node-2".into(), make_req("node-2")).await;
        assert!(hb.untrack_exact("node-2", "node-2-incarnation").await);

        let guard = hb.registry.read().await;
        let stored = guard.get("node-1").expect("node-1 must still be tracked");
        assert_eq!(stored.hostname, req.hostname);
        assert_eq!(stored.address, req.address);
        assert_eq!(stored.port, req.port);
    }
}
