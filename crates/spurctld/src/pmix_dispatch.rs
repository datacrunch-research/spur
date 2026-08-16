// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

//! Shared multi-node PMIx prepare / release helpers for batch launch and srun steps.

use tracing::{error, warn};

use spur_core::node::NodeSource;
use spur_proto::proto::slurm_agent_client::SlurmAgentClient;
use spur_proto::proto::{PreparePmixRequest, ReleasePmixRequest};

pub const MULTI_NODE_PMIX_K8S_UNSUPPORTED: &str =
    "multi-node PMIx is not supported on K8s virtual agents";

/// Reject multi-node PMIx at submit when the user pins a K8s virtual agent.
pub fn validate_multi_node_pmix_nodelist(
    mpi: &str,
    num_nodes: u32,
    nodelist: Option<&str>,
    node_source: impl Fn(&str) -> Option<NodeSource>,
) -> Result<(), String> {
    if mpi != spur_core::mpi::MPI_PMIX || num_nodes <= 1 {
        return Ok(());
    }
    let Some(nodelist) = nodelist.filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    for name in nodelist.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if node_source(name).is_some_and(|source| matches!(source, NodeSource::Kubernetes { .. })) {
            return Err(MULTI_NODE_PMIX_K8S_UNSUPPORTED.into());
        }
    }
    Ok(())
}

/// Returns an error detail when any node is a K8s virtual agent.
pub fn multi_node_pmix_unsupported(
    sources: impl IntoIterator<Item = NodeSource>,
) -> Option<String> {
    for source in sources {
        if matches!(source, NodeSource::Kubernetes { .. }) {
            return Some(MULTI_NODE_PMIX_K8S_UNSUPPORTED.into());
        }
    }
    None
}

/// One agent target for a parallel PreparePmix RPC.
pub struct PmixPrepareNode {
    pub node_name: String,
    pub agent_addr: String,
    pub worker_incarnation: String,
    pub pmix_plan: spur_proto::proto::PmixLaunchPlan,
}

/// Stable opaque identity for a batch PMIx prepare. The token is carried by
/// Prepare, Launch, Release, and exact-attempt Cancel paths so none of them can
/// consume or tear down a different prepare for the same job id.
pub fn batch_prepare_token(submission_generation: &str, run_attempt: u32) -> String {
    format!("batch:{submission_generation}:{run_attempt}")
}

/// Stable opaque identity for one srun step PMIx prepare.
pub fn step_prepare_token(submission_generation: &str, run_attempt: u32, step_id: u32) -> String {
    format!("step:{submission_generation}:{run_attempt}:{step_id}")
}

pub async fn prepare_pmix_on_agent(
    agent_addr: &str,
    job_id: u32,
    submission_generation: &str,
    worker_incarnation: &str,
    run_attempt: u32,
    prepare_token: &str,
    pmix_plan: spur_proto::proto::PmixLaunchPlan,
) -> Result<(), String> {
    let mut client = SlurmAgentClient::connect(agent_addr.to_string())
        .await
        .map_err(|e| format!("connect failed: {e}"))?
        .max_decoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(spur_proto::MAX_GRPC_REQUEST_SIZE);
    let resp = client
        .prepare_pmix(PreparePmixRequest {
            job_id,
            pmix_plan: Some(pmix_plan),
            run_attempt,
            prepare_token: prepare_token.to_string(),
            submission_generation: submission_generation.to_string(),
            worker_incarnation: worker_incarnation.to_string(),
        })
        .await
        .map_err(|e| format!("PreparePmix RPC failed: {e}"))?
        .into_inner();
    if resp.success {
        Ok(())
    } else if resp.error.is_empty() {
        Err("PreparePmix rejected without detail".into())
    } else {
        Err(resp.error)
    }
}

pub async fn release_pmix_on_agent(
    agent_addr: &str,
    job_id: u32,
    submission_generation: &str,
    worker_incarnation: &str,
    run_attempt: u32,
    prepare_token: &str,
) {
    let result = async {
        let mut client = SlurmAgentClient::connect(agent_addr.to_string())
            .await
            .map_err(|e| tonic::Status::unavailable(e.to_string()))?
            .max_decoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE)
            .max_encoding_message_size(spur_proto::MAX_GRPC_REQUEST_SIZE);
        client
            .release_pmix(ReleasePmixRequest {
                job_id,
                run_attempt,
                prepare_token: prepare_token.to_string(),
                submission_generation: submission_generation.to_string(),
                worker_incarnation: worker_incarnation.to_string(),
            })
            .await?;
        Ok::<(), tonic::Status>(())
    }
    .await;
    if let Err(e) = result {
        warn!(job_id, run_attempt, prepare_token, agent = %agent_addr, error = %e, "ReleasePmix rollback failed");
    }
}

pub async fn release_pmix_on_agents(
    agents: &[(String, String)],
    job_id: u32,
    submission_generation: &str,
    run_attempt: u32,
    prepare_token: &str,
) {
    let mut release_set = tokio::task::JoinSet::new();
    for (agent_addr, worker_incarnation) in agents {
        let agent_addr = agent_addr.clone();
        let worker_incarnation = worker_incarnation.clone();
        let submission_generation = submission_generation.to_string();
        let prepare_token = prepare_token.to_string();
        release_set.spawn(async move {
            release_pmix_on_agent(
                &agent_addr,
                job_id,
                &submission_generation,
                &worker_incarnation,
                run_attempt,
                &prepare_token,
            )
            .await;
        });
    }
    while release_set.join_next().await.is_some() {}
}

/// Parallel PreparePmix on all nodes. `rollback_on_failure` is for srun step
/// setup, where no durable batch-dispatch abort fence exists. Batch dispatch
/// passes false: its exact-attempt CancelJob recovery is the only operation
/// authorized to release a prepare, after JobDispatchAbortBegin commits.
pub async fn prepare_pmix_on_nodes(
    job_id: u32,
    submission_generation: &str,
    run_attempt: u32,
    prepare_token: &str,
    nodes: Vec<PmixPrepareNode>,
    rollback_on_failure: bool,
) -> Result<(), String> {
    if nodes.is_empty() {
        return Ok(());
    }

    let all_agents: Vec<(String, String)> = nodes
        .iter()
        .map(|n| (n.agent_addr.clone(), n.worker_incarnation.clone()))
        .collect();

    let mut prepare_set = tokio::task::JoinSet::new();
    for node in nodes {
        let agent_addr = node.agent_addr.clone();
        let node_name = node.node_name.clone();
        let pmix_plan = node.pmix_plan;
        let worker_incarnation = node.worker_incarnation;
        let submission_generation = submission_generation.to_string();
        let prepare_token = prepare_token.to_string();
        prepare_set.spawn(async move {
            prepare_pmix_on_agent(
                &agent_addr,
                job_id,
                &submission_generation,
                &worker_incarnation,
                run_attempt,
                &prepare_token,
                pmix_plan,
            )
            .await
            .map(|()| agent_addr)
            .map_err(|e| format!("{node_name}: {e}"))
        });
    }

    let mut errors: Vec<String> = Vec::new();
    while let Some(result) = prepare_set.join_next().await {
        match result {
            Ok(Ok(_agent_addr)) => {}
            Ok(Err(e)) => errors.push(e),
            Err(e) => errors.push(format!("prepare task panicked: {e}")),
        }
    }

    if errors.is_empty() {
        return Ok(());
    }

    let detail = errors.join("; ");
    error!(job_id, run_attempt, prepare_token, error = %detail, "PMIx prepare failed");
    if rollback_on_failure {
        release_pmix_on_agents(
            &all_agents,
            job_id,
            submission_generation,
            run_attempt,
            prepare_token,
        )
        .await;
    }
    Err(detail)
}

/// Rolls back controller-side PMIx prepare when an srun step handler is cancelled
/// before the normal release path runs.
pub struct PmixPreparedReleaseGuard {
    job_id: u32,
    submission_generation: String,
    run_attempt: u32,
    prepare_token: String,
    agents: Vec<(String, String)>,
    release: bool,
}

impl PmixPreparedReleaseGuard {
    pub fn new(
        job_id: u32,
        submission_generation: String,
        run_attempt: u32,
        prepare_token: String,
        agents: Vec<(String, String)>,
    ) -> Self {
        Self {
            job_id,
            submission_generation,
            run_attempt,
            prepare_token,
            agents,
            release: true,
        }
    }

    pub fn disarm(&mut self) {
        self.release = false;
    }
}

impl Drop for PmixPreparedReleaseGuard {
    fn drop(&mut self) {
        if !self.release {
            return;
        }
        let job_id = self.job_id;
        let run_attempt = self.run_attempt;
        let prepare_token = self.prepare_token.clone();
        let agents = self.agents.clone();
        let submission_generation = self.submission_generation.clone();
        tokio::spawn(async move {
            release_pmix_on_agents(
                &agents,
                job_id,
                &submission_generation,
                run_attempt,
                &prepare_token,
            )
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spur_core::node::NodeSource;

    #[test]
    fn multi_node_pmix_unsupported_on_k8s_agents() {
        let err = multi_node_pmix_unsupported([NodeSource::Kubernetes {
            namespace: "spur-ci".into(),
        }]);
        assert_eq!(err.as_deref(), Some(MULTI_NODE_PMIX_K8S_UNSUPPORTED));
    }

    #[test]
    fn multi_node_pmix_allowed_on_native_hosts() {
        let err = multi_node_pmix_unsupported([NodeSource::NativeHost]);
        assert!(err.is_none());
    }

    #[test]
    fn prepare_tokens_are_stable_and_scope_batch_from_steps() {
        assert_eq!(
            batch_prepare_token("generation-a", 7),
            "batch:generation-a:7"
        );
        assert_eq!(
            step_prepare_token("generation-a", 7, 3),
            "step:generation-a:7:3"
        );
        assert_ne!(
            batch_prepare_token("generation-a", 7),
            step_prepare_token("generation-a", 7, 3)
        );
        assert_ne!(
            step_prepare_token("generation-a", 7, 3),
            step_prepare_token("generation-a", 7, 4)
        );
        assert_ne!(
            batch_prepare_token("generation-a", 7),
            batch_prepare_token("generation-b", 7)
        );
    }

    #[test]
    fn multi_node_pmix_nodelist_rejects_k8s_agent_at_submit() {
        let err = validate_multi_node_pmix_nodelist(
            spur_core::mpi::MPI_PMIX,
            2,
            Some("k8s-worker1"),
            |name| {
                if name == "k8s-worker1" {
                    Some(NodeSource::Kubernetes {
                        namespace: "spur-ci".into(),
                    })
                } else {
                    Some(NodeSource::NativeHost)
                }
            },
        );
        assert_eq!(err.unwrap_err(), MULTI_NODE_PMIX_K8S_UNSUPPORTED);
    }

    #[test]
    fn multi_node_pmix_nodelist_allows_native_hosts_at_submit() {
        assert!(validate_multi_node_pmix_nodelist(
            spur_core::mpi::MPI_PMIX,
            2,
            Some("n1,n2"),
            |_| Some(NodeSource::NativeHost),
        )
        .is_ok());
    }
}
