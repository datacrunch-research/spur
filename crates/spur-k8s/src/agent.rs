// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use k8s_openapi::api::core::v1::{
    Container, EnvVar, HostPathVolumeSource, Pod, PodSpec, ResourceRequirements, Service,
    ServicePort, ServiceSpec, Volume, VolumeMount,
};
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{
    Api, AttachParams, DeleteParams, ListParams, ObjectMeta, Patch, PatchParams, PostParams,
    Preconditions,
};
use kube::Client;
use tokio::io::AsyncReadExt;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info, warn};

use crate::crd::{
    launch_spec_sha256, resolved_submission_user, validate_preview_launch_fields, SpurJob,
    SpurJobStatus,
};
use crate::execution_identity::{
    PodExecutionIdentity, JOB_ID_LABEL, PROVENANCE_RECORDED_ANNOTATION, RUN_ATTEMPT_ANNOTATION,
    RUN_ATTEMPT_LABEL, SERVICE_DISPATCH_TOKEN_ANNOTATION, SUBMISSION_GENERATION_ANNOTATION,
    SUBMISSION_GENERATION_LABEL,
};
use crate::heartbeat::HeartbeatManager;
use spur_core::spur_env::SpurEnv;
use spur_proto::proto::slurm_agent_server::SlurmAgent;
use spur_proto::proto::*;

const NS_LOOKUP_BUDGET: Duration = Duration::from_secs(5);

fn resource_has_controller_owner(metadata: &ObjectMeta, owner_uid: &str) -> bool {
    metadata.owner_references.as_ref().is_some_and(|owners| {
        owners
            .iter()
            .any(|owner| owner.uid == owner_uid && owner.controller == Some(true))
    })
}

fn status_matches_execution(status: &SpurJobStatus, identity: &PodExecutionIdentity) -> bool {
    status.spur_job_id == Some(identity.key.job_id)
        && status.submission_generation.as_deref()
            == Some(identity.key.submission_generation.as_str())
        && status
            .submission_token
            .as_deref()
            .is_some_and(|token| !token.is_empty())
}

/// Mutable Pod metadata is never sufficient for a worker-control operation.
/// The exact Pod must also be bound by immutable UID and dispatch nonce in the
/// status of its controller-owned SpurJob.
fn status_binds_control_pod(
    status: &SpurJobStatus,
    owner_uid: &str,
    requested: &PodExecutionIdentity,
    pod: &Pod,
) -> bool {
    if !status_matches_execution(status, requested)
        || !resource_has_controller_owner(&pod.metadata, owner_uid)
        || pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(PROVENANCE_RECORDED_ANNOTATION))
            .is_none_or(|value| value != "true")
    {
        return false;
    }

    let Some(name) = pod.metadata.name.as_deref() else {
        return false;
    };
    let Some(uid) = pod.metadata.uid.as_deref() else {
        return false;
    };
    let Ok(found) = PodExecutionIdentity::from_pod(pod) else {
        return false;
    };
    found.key == requested.key
        && found.worker_incarnation == requested.worker_incarnation
        && status.submission_token.as_deref() == Some(found.submission_token.as_str())
        && !found.pod_dispatch_token.is_empty()
        && status
            .pod_dispatch_tokens
            .get(name)
            .is_some_and(|token| token == &found.pod_dispatch_token)
        && status
            .pod_uids
            .get(name)
            .is_some_and(|recorded_uid| recorded_uid == uid)
}

fn status_binds_control_service(
    status: &SpurJobStatus,
    owner_uid: &str,
    identity: &PodExecutionIdentity,
    service: &Service,
) -> bool {
    if !status_matches_execution(status, identity)
        || !resource_has_controller_owner(&service.metadata, owner_uid)
        || !identity.owns_service(service)
    {
        return false;
    }
    let Some(name) = service.metadata.name.as_deref() else {
        return false;
    };
    let Some(uid) = service.metadata.uid.as_deref() else {
        return false;
    };
    status
        .service_uids
        .get(name)
        .is_some_and(|recorded_uid| recorded_uid == uid)
}

fn status_binds_cleanup_pod(
    status: &SpurJobStatus,
    owner_uid: &str,
    requested: &PodExecutionIdentity,
    pod: &Pod,
) -> bool {
    if !status_matches_execution(status, requested)
        || !resource_has_controller_owner(&pod.metadata, owner_uid)
    {
        return false;
    }
    let Some(name) = pod.metadata.name.as_deref() else {
        return false;
    };
    let Ok(found) = PodExecutionIdentity::from_pod(pod) else {
        return false;
    };
    found.key == requested.key
        && found.worker_incarnation == requested.worker_incarnation
        && status.submission_token.as_deref() == Some(found.submission_token.as_str())
        && !found.pod_dispatch_token.is_empty()
        && status.pod_dispatch_tokens.get(name) == Some(&found.pod_dispatch_token)
        && pod.metadata.uid.as_deref().is_some_and(|uid| {
            status
                .pod_uids
                .get(name)
                .is_none_or(|recorded| recorded == uid)
        })
}

fn status_binds_cleanup_service(
    status: &SpurJobStatus,
    owner_uid: &str,
    identity: &PodExecutionIdentity,
    service: &Service,
) -> bool {
    if !status_matches_execution(status, identity)
        || !resource_has_controller_owner(&service.metadata, owner_uid)
        || !identity.owns_service(service)
    {
        return false;
    }
    let (Some(name), Some(uid)) = (
        service.metadata.name.as_deref(),
        service.metadata.uid.as_deref(),
    ) else {
        return false;
    };
    let service_token = service
        .metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(SERVICE_DISPATCH_TOKEN_ANNOTATION));
    service_token.is_some_and(|token| status.service_dispatch_tokens.get(name) == Some(token))
        && status
            .service_uids
            .get(name)
            .is_none_or(|recorded| recorded == uid)
}

fn classify_control_pods(
    status: &SpurJobStatus,
    owner_uid: &str,
    requested: &PodExecutionIdentity,
    pods: Vec<Pod>,
    allow_cleanup_recovery: bool,
) -> Result<Vec<Pod>, String> {
    let mut exact = Vec::new();
    for pod in pods {
        // The numeric job label is forgeable. Foreign-owner objects are not
        // candidates and must not be allowed to denial-of-service exact control
        // by carrying malformed identity annotations.
        if !resource_has_controller_owner(&pod.metadata, owner_uid) {
            continue;
        }
        match PodExecutionIdentity::from_pod(&pod) {
            Ok(found)
                if found.key == requested.key
                    && found.worker_incarnation == requested.worker_incarnation =>
            {
                if status_binds_control_pod(status, owner_uid, requested, &pod)
                    || (allow_cleanup_recovery
                        && status_binds_cleanup_pod(status, owner_uid, requested, &pod))
                {
                    exact.push(pod);
                } else {
                    return Err(format!(
                        "Pod {} claims the requested execution but lacks immutable CR provenance",
                        pod.metadata.name.as_deref().unwrap_or("<unnamed>")
                    ));
                }
            }
            Ok(_) => {}
            Err(error) => {
                return Err(format!(
                    "numeric-job Pod {} has ambiguous legacy identity: {error}",
                    pod.metadata.name.as_deref().unwrap_or("<unnamed>")
                ));
            }
        }
    }
    Ok(exact)
}

fn service_claims_execution(service: &Service, identity: &PodExecutionIdentity) -> Option<bool> {
    let annotations = service.metadata.annotations.as_ref()?;
    let generation = annotations.get(SUBMISSION_GENERATION_ANNOTATION)?;
    let attempt = annotations
        .get(RUN_ATTEMPT_ANNOTATION)?
        .parse::<u32>()
        .ok()?;
    Some(generation == &identity.key.submission_generation && attempt == identity.key.run_attempt)
}

fn classify_control_services(
    status: &SpurJobStatus,
    owner_uid: &str,
    identity: &PodExecutionIdentity,
    services: Vec<Service>,
    allow_cleanup_recovery: bool,
) -> Result<Vec<Service>, String> {
    let mut exact = Vec::new();
    for service in services {
        if !resource_has_controller_owner(&service.metadata, owner_uid) {
            continue;
        }
        match service_claims_execution(&service, identity) {
            Some(true) if status_binds_control_service(status, owner_uid, identity, &service) => {
                exact.push(service);
            }
            Some(true)
                if allow_cleanup_recovery
                    && status_binds_cleanup_service(status, owner_uid, identity, &service) =>
            {
                exact.push(service);
            }
            Some(true) => {
                return Err(format!(
                    "Service {} claims the requested execution but lacks immutable CR provenance",
                    service.metadata.name.as_deref().unwrap_or("<unnamed>")
                ));
            }
            Some(false) => {}
            None => {
                return Err(format!(
                    "numeric-job Service {} has ambiguous legacy identity",
                    service.metadata.name.as_deref().unwrap_or("<unnamed>")
                ));
            }
        }
    }
    Ok(exact)
}

/// Virtual SlurmAgent that creates K8s Pods instead of fork/exec.
pub struct VirtualAgent {
    client: Client,
    heartbeat: Arc<HeartbeatManager>,
}

impl VirtualAgent {
    pub fn new(client: Client, heartbeat: Arc<HeartbeatManager>) -> Self {
        Self { client, heartbeat }
    }

    /// Resolve exactly one owning CR. Numeric ID alone is never sufficient.
    /// Launch carries the token and must match it; worker-control requests do
    /// not, so they require a nonempty durable token and hydrate it from the
    /// unique exact-generation CR before touching any resource.
    async fn resolve_spur_job(&self, identity: &PodExecutionIdentity) -> Result<SpurJob, Status> {
        let api: Api<SpurJob> = Api::all(self.client.clone());
        let job_id = identity.key.job_id;
        let lp = ListParams::default().labels(&format!("{JOB_ID_LABEL}={job_id}"));

        let result = tokio::time::timeout(
            NS_LOOKUP_BUDGET,
            (|| async {
                let list = tokio::time::timeout(Duration::from_millis(300), api.list(&lp))
                    .await
                    .map_err(|_| Status::unavailable("k8s API timeout"))?
                    .map_err(|e| Status::internal(e.to_string()))?;

                let matches: Vec<SpurJob> = list
                    .items
                    .into_iter()
                    .filter(|job| {
                        let status = job.status.as_ref();
                        status.and_then(|status| status.spur_job_id) == Some(job_id)
                            && status.and_then(|status| status.submission_generation.as_deref())
                                == Some(identity.key.submission_generation.as_str())
                            && status
                                .and_then(|status| status.submission_token.as_deref())
                                .is_some_and(|token| {
                                    !token.is_empty()
                                        && (identity.submission_token.is_empty()
                                            || token == identity.submission_token)
                                })
                            && job
                                .metadata
                                .labels
                                .as_ref()
                                .and_then(|labels| labels.get(SUBMISSION_GENERATION_LABEL))
                                == Some(&identity.key.submission_generation)
                    })
                    .collect();
                let mut matches = matches.into_iter();
                match (matches.next(), matches.next()) {
                    (None, _) => Err(Status::not_found(format!(
                        "exact SpurJob identity for job {job_id} is not yet visible"
                    ))),
                    (Some(job), None) => Ok(job),
                    (Some(_), Some(_)) => Err(Status::failed_precondition(format!(
                        "exact job {job_id} identity matches multiple SpurJobs"
                    ))),
                }
            })
            .retry(
                ExponentialBuilder::default()
                    .with_min_delay(Duration::from_millis(200))
                    .with_max_delay(Duration::from_secs(2))
                    .without_max_times(),
            )
            .when(|e: &Status| {
                matches!(e.code(), tonic::Code::Unavailable | tonic::Code::NotFound)
            }),
        )
        .await;

        match result {
            Ok(Ok(job)) => Ok(job),
            Ok(Err(status)) => Err(status),
            Err(_elapsed) => Err(Status::deadline_exceeded(format!(
                "exact SpurJob lookup for job {job_id} timed out after {}s",
                NS_LOOKUP_BUDGET.as_secs()
            ))),
        }
    }

    fn hydrate_control_identity(
        owner: &SpurJob,
        requested: &PodExecutionIdentity,
    ) -> Result<PodExecutionIdentity, Status> {
        let status = owner
            .status
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no durable status"))?;
        if !status_matches_execution(status, requested) {
            return Err(Status::failed_precondition(
                "owning SpurJob does not bind the requested execution",
            ));
        }
        let submission_token = status
            .submission_token
            .as_deref()
            .filter(|token| !token.is_empty())
            .ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no durable submission token")
            })?;
        let mut exact = requested.clone();
        exact.submission_token = submission_token.to_string();
        Ok(exact)
    }

    fn owner_reference(job: &SpurJob) -> Result<OwnerReference, Status> {
        Ok(OwnerReference {
            api_version: "spur.amd.com/v1alpha1".to_string(),
            kind: "SpurJob".to_string(),
            name: job
                .metadata
                .name
                .clone()
                .ok_or_else(|| Status::failed_precondition("owning SpurJob has no name"))?,
            uid: job
                .metadata
                .uid
                .clone()
                .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?,
            controller: Some(true),
            block_owner_deletion: Some(true),
        })
    }

    async fn ensure_pod_dispatch_token(
        &self,
        owner: &SpurJob,
        identity: &PodExecutionIdentity,
        pod_name: &str,
    ) -> Result<String, Status> {
        let namespace = owner
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_name = owner
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no name"))?;
        let owner_uid = owner
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
        let jobs: Api<SpurJob> = Api::namespaced(self.client.clone(), namespace);

        for _ in 0..8 {
            let fresh = jobs
                .get(owner_name)
                .await
                .map_err(|error| Status::unavailable(format!("failed to read SpurJob: {error}")))?;
            if fresh.metadata.uid.as_deref() != Some(owner_uid) {
                return Err(Status::failed_precondition(
                    "owning SpurJob was replaced while dispatching",
                ));
            }
            let status = fresh.status.as_ref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no durable status")
            })?;
            if status.spur_job_id != Some(identity.key.job_id)
                || status.submission_generation.as_deref()
                    != Some(identity.key.submission_generation.as_str())
                || status.submission_token.as_deref() != Some(identity.submission_token.as_str())
            {
                return Err(Status::failed_precondition(
                    "owning SpurJob identity changed while dispatching",
                ));
            }
            if let Some(token) = status
                .pod_dispatch_tokens
                .get(pod_name)
                .filter(|token| !token.is_empty())
            {
                return Ok(token.clone());
            }
            let resource_version = fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no resourceVersion")
            })?;
            let token = spur_core::job::Uuid::new_v4().to_string();
            let patch = serde_json::json!({
                "metadata": { "resourceVersion": resource_version },
                "status": { "podDispatchTokens": { (pod_name): token } }
            });
            match jobs
                .patch_status(owner_name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => return Ok(token),
                Err(kube::Error::Api(error)) if error.code == 409 => continue,
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "failed to persist Pod dispatch token: {error}"
                    )));
                }
            }
        }
        Err(Status::aborted(
            "concurrent Pod dispatch token updates did not converge",
        ))
    }

    async fn ensure_service_dispatch_token(
        &self,
        owner: &SpurJob,
        identity: &PodExecutionIdentity,
        service_name: &str,
    ) -> Result<String, Status> {
        let namespace = owner
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_name = owner
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no name"))?;
        let owner_uid = owner
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
        let jobs: Api<SpurJob> = Api::namespaced(self.client.clone(), namespace);

        for _ in 0..8 {
            let fresh = jobs
                .get(owner_name)
                .await
                .map_err(|error| Status::unavailable(format!("failed to read SpurJob: {error}")))?;
            if fresh.metadata.uid.as_deref() != Some(owner_uid) {
                return Err(Status::failed_precondition(
                    "owning SpurJob was replaced while creating Service",
                ));
            }
            let status = fresh.status.as_ref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no durable status")
            })?;
            if !status_matches_execution(status, identity)
                || status.submission_token.as_deref() != Some(identity.submission_token.as_str())
            {
                return Err(Status::failed_precondition(
                    "owning SpurJob identity changed while creating Service",
                ));
            }
            if let Some(token) = status
                .service_dispatch_tokens
                .get(service_name)
                .filter(|token| !token.is_empty())
            {
                return Ok(token.clone());
            }
            let resource_version = fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no resourceVersion")
            })?;
            let token = spur_core::job::Uuid::new_v4().to_string();
            let patch = serde_json::json!({
                "metadata": { "resourceVersion": resource_version },
                "status": { "serviceDispatchTokens": { (service_name): token } }
            });
            match jobs
                .patch_status(owner_name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => return Ok(token),
                Err(kube::Error::Api(error)) if error.code == 409 => continue,
                Err(error) => {
                    return Err(Status::unavailable(format!(
                        "failed to persist Service dispatch token: {error}"
                    )));
                }
            }
        }
        Err(Status::aborted(
            "concurrent Service dispatch token updates did not converge",
        ))
    }

    async fn record_pod_provenance(&self, owner: &SpurJob, pod: &Pod) -> Result<(), Status> {
        let namespace = owner
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_name = owner
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no name"))?;
        let pod_name = pod
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("created Pod has no name"))?;
        let pod_uid = pod
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("created Pod has no UID"))?;
        let owner_uid = owner
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
        if !resource_has_controller_owner(&pod.metadata, owner_uid) {
            return Err(Status::failed_precondition(
                "created Pod is not controller-owned by the exact SpurJob UID",
            ));
        }

        let parsed = PodExecutionIdentity::from_pod(pod).map_err(Status::failed_precondition)?;
        if parsed.pod_dispatch_token.is_empty() {
            return Err(Status::failed_precondition(
                "created Pod has no durable dispatch token",
            ));
        }
        let jobs: Api<SpurJob> = Api::namespaced(self.client.clone(), namespace);
        let mut provenance_recorded = false;
        for _ in 0..8 {
            let fresh = jobs.get(owner_name).await.map_err(|error| {
                Status::unavailable(format!("failed to verify SpurJob: {error}"))
            })?;
            if fresh.metadata.uid.as_deref() != Some(owner_uid) {
                return Err(Status::failed_precondition(
                    "owning SpurJob was replaced before Pod provenance was recorded",
                ));
            }
            let fresh_status = fresh.status.as_ref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no durable status")
            })?;
            if fresh_status.spur_job_id != Some(parsed.key.job_id)
                || fresh_status.submission_generation.as_deref()
                    != Some(parsed.key.submission_generation.as_str())
                || fresh_status.submission_token.as_deref()
                    != Some(parsed.submission_token.as_str())
            {
                return Err(Status::failed_precondition(
                    "Pod identity no longer matches owning SpurJob status",
                ));
            }
            if fresh_status.pod_dispatch_tokens.get(pod_name) != Some(&parsed.pod_dispatch_token) {
                return Err(Status::failed_precondition(
                    "Pod dispatch token is not bound in operator state",
                ));
            }
            match fresh_status.pod_uids.get(pod_name) {
                Some(recorded_uid) if recorded_uid == pod_uid => {
                    provenance_recorded = true;
                    break;
                }
                Some(_) => {
                    return Err(Status::failed_precondition(
                        "Pod name is already bound to a different immutable UID",
                    ));
                }
                None => {
                    let resource_version =
                        fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                            Status::failed_precondition("owning SpurJob has no resourceVersion")
                        })?;
                    let status_patch = serde_json::json!({
                        "metadata": { "resourceVersion": resource_version },
                        "status": { "podUids": { (pod_name): pod_uid } }
                    });
                    match jobs
                        .patch_status(
                            owner_name,
                            &PatchParams::default(),
                            &Patch::Merge(&status_patch),
                        )
                        .await
                    {
                        Ok(_) => {
                            provenance_recorded = true;
                            break;
                        }
                        Err(kube::Error::Api(error)) if error.code == 409 => continue,
                        Err(error) => {
                            return Err(Status::unavailable(format!(
                                "failed to record Pod UID: {error}"
                            )));
                        }
                    }
                }
            }
        }
        if !provenance_recorded {
            return Err(Status::aborted(
                "concurrent Pod UID provenance updates did not converge",
            ));
        }

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let pod_patch = serde_json::json!({
            "metadata": { "annotations": { PROVENANCE_RECORDED_ANNOTATION: "true" } }
        });
        pods.patch(pod_name, &PatchParams::default(), &Patch::Merge(&pod_patch))
            .await
            .map_err(|error| {
                Status::unavailable(format!("failed to publish Pod provenance: {error}"))
            })?;
        Ok(())
    }

    async fn record_service_provenance(
        &self,
        owner: &SpurJob,
        identity: &PodExecutionIdentity,
        service: &Service,
    ) -> Result<(), Status> {
        let namespace = owner
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_name = owner
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no name"))?;
        let owner_uid = owner
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
        let service_name = service
            .metadata
            .name
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("created Service has no name"))?;
        let service_uid = service
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("created Service has no UID"))?;
        if !resource_has_controller_owner(&service.metadata, owner_uid)
            || !identity.owns_service(service)
        {
            return Err(Status::failed_precondition(
                "created Service lacks exact immutable execution ownership",
            ));
        }

        let jobs: Api<SpurJob> = Api::namespaced(self.client.clone(), namespace);
        for _ in 0..8 {
            let fresh = jobs.get(owner_name).await.map_err(|error| {
                Status::unavailable(format!("failed to verify SpurJob: {error}"))
            })?;
            if fresh.metadata.uid.as_deref() != Some(owner_uid) {
                return Err(Status::failed_precondition(
                    "owning SpurJob was replaced before Service provenance was recorded",
                ));
            }
            let status = fresh.status.as_ref().ok_or_else(|| {
                Status::failed_precondition("owning SpurJob has no durable status")
            })?;
            if !status_matches_execution(status, identity)
                || status.submission_token.as_deref() != Some(identity.submission_token.as_str())
            {
                return Err(Status::failed_precondition(
                    "Service identity no longer matches owning SpurJob status",
                ));
            }
            let service_dispatch_token = service
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(SERVICE_DISPATCH_TOKEN_ANNOTATION))
                .filter(|token| !token.is_empty())
                .ok_or_else(|| {
                    Status::failed_precondition("created Service has no durable dispatch token")
                })?;
            if status.service_dispatch_tokens.get(service_name) != Some(service_dispatch_token) {
                return Err(Status::failed_precondition(
                    "Service dispatch token is not bound in operator state",
                ));
            }
            match status.service_uids.get(service_name) {
                Some(recorded_uid) if recorded_uid == service_uid => return Ok(()),
                Some(_) => {
                    return Err(Status::failed_precondition(
                        "Service name is already bound to a different immutable UID",
                    ));
                }
                None => {
                    let resource_version =
                        fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                            Status::failed_precondition("owning SpurJob has no resourceVersion")
                        })?;
                    let patch = serde_json::json!({
                        "metadata": { "resourceVersion": resource_version },
                        "status": { "serviceUids": { (service_name): service_uid } }
                    });
                    match jobs
                        .patch_status(owner_name, &PatchParams::default(), &Patch::Merge(&patch))
                        .await
                    {
                        Ok(_) => return Ok(()),
                        Err(kube::Error::Api(error)) if error.code == 409 => continue,
                        Err(error) => {
                            return Err(Status::unavailable(format!(
                                "failed to record Service UID: {error}"
                            )));
                        }
                    }
                }
            }
        }
        Err(Status::aborted(
            "concurrent Service UID provenance updates did not converge",
        ))
    }

    async fn require_current_identity(
        &self,
        identity: &PodExecutionIdentity,
    ) -> Result<String, Status> {
        self.heartbeat
            .require_known_incarnation(&identity.worker_incarnation)
            .await
    }

    async fn list_exact_pods(
        &self,
        identity: &PodExecutionIdentity,
        allow_cleanup_recovery: bool,
    ) -> Result<Vec<Pod>, Status> {
        let owner = self.resolve_spur_job(identity).await?;
        let namespace = owner
            .metadata
            .namespace
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_uid = owner
            .metadata
            .uid
            .as_deref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
        let status = owner
            .status
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no durable status"))?;
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
        let lp = ListParams::default().labels(&format!("{JOB_ID_LABEL}={}", identity.key.job_id));
        let listed = pods
            .list(&lp)
            .await
            .map_err(|error| Status::internal(format!("failed to list exact job Pods: {error}")))?;
        classify_control_pods(
            status,
            owner_uid,
            identity,
            listed.items,
            allow_cleanup_recovery,
        )
        .map_err(Status::failed_precondition)
    }

    async fn find_exact_pod(&self, identity: &PodExecutionIdentity) -> Result<Pod, Status> {
        self.require_current_identity(identity).await?;
        let mut pods = self.list_exact_pods(identity, false).await?;
        match pods.len() {
            0 => Err(Status::not_found(format!(
                "exact execution of job {} is not running on this virtual worker",
                identity.key.job_id
            ))),
            1 => pods
                .pop()
                .ok_or_else(|| Status::internal("exact Pod disappeared from the in-memory result")),
            count => Err(Status::failed_precondition(format!(
                "exact execution of job {} owns {count} Pods on one virtual worker",
                identity.key.job_id
            ))),
        }
    }
}

#[tonic::async_trait]
impl SlurmAgent for VirtualAgent {
    type StreamJobOutputStream =
        tokio_stream::wrappers::ReceiverStream<Result<StreamJobOutputChunk, Status>>;
    type InteractiveSessionStream =
        tokio_stream::wrappers::ReceiverStream<Result<InteractiveOutput, Status>>;

    async fn launch_job(
        &self,
        request: Request<LaunchJobRequest>,
    ) -> Result<Response<LaunchJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let target_node = req.target_node.clone();
        if target_node.is_empty() {
            return Err(Status::invalid_argument(
                "Kubernetes LaunchJob requires target_node",
            ));
        }
        let mut identity = PodExecutionIdentity::from_launch_request(
            job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
            &req.submission_token,
        )?;
        self.heartbeat
            .require_incarnation(&target_node, &identity.worker_incarnation)
            .await?;
        let spur_job = self.resolve_spur_job(&identity).await?;
        validate_preview_launch_fields(&spur_job.spec).map_err(Status::failed_precondition)?;
        let current_launch_digest =
            launch_spec_sha256(&spur_job.spec, &resolved_submission_user(&spur_job))
                .map_err(Status::failed_precondition)?;
        if spur_job
            .status
            .as_ref()
            .and_then(|status| status.launch_spec_sha256.as_deref())
            != Some(current_launch_digest.as_str())
        {
            return Err(Status::failed_precondition(
                "SpurJob spec changed after submission intent was persisted",
            ));
        }
        let ns = spur_job
            .metadata
            .namespace
            .clone()
            .ok_or_else(|| Status::failed_precondition("owning SpurJob has no namespace"))?;
        let owner_reference = Self::owner_reference(&spur_job)?;
        let peer_nodes = &req.peer_nodes;
        let num_peers = peer_nodes.len();
        let headless_service_name = (num_peers > 1).then(|| execution_service_name(&identity));

        let spec = req
            .spec
            .ok_or_else(|| Status::invalid_argument("missing job spec"))?;

        // Include a stable digest of the full immutable execution identity.
        // Exec/log APIs address Pods by name and do not accept a UID
        // precondition, so numeric-job-ID names could race deletion and attach
        // to a replacement Pod between exact lookup and stream establishment.
        let pod_name = execution_pod_name(&identity, &target_node);
        identity.pod_dispatch_token = self
            .ensure_pod_dispatch_token(&spur_job, &identity, &pod_name)
            .await?;

        let image = if spec.container_image.is_empty() {
            "busybox:latest".to_string()
        } else {
            spec.container_image.clone()
        };

        // Build resource requests
        let mut resource_requests = BTreeMap::new();
        let mut resource_limits = BTreeMap::new();

        if let Some(ref alloc) = req.allocated {
            if alloc.cpus > 0 {
                let cpu_str = alloc.cpus.to_string();
                resource_requests.insert("cpu".to_string(), Quantity(cpu_str.clone()));
                resource_limits.insert("cpu".to_string(), Quantity(cpu_str));
            }
            if alloc.memory_mb > 0 {
                let mem_str = format!("{}Mi", alloc.memory_mb);
                resource_requests.insert("memory".to_string(), Quantity(mem_str.clone()));
                resource_limits.insert("memory".to_string(), Quantity(mem_str));
            }
            let gpu_count = alloc
                .devices
                .get("gpu")
                .map(|d| d.devices.len() as u32)
                .unwrap_or(0);
            if gpu_count > 0 {
                let gpu_str = gpu_count.to_string();
                let gpu_type = spec
                    .gres
                    .iter()
                    .find_map(|g| spur_core::resource::parse_gres(g))
                    .and_then(|(_, t, _)| t);
                let gpu_resource_key = gpu_vendor_resource_key(gpu_type.as_deref());
                resource_limits.insert(gpu_resource_key.to_string(), Quantity(gpu_str.clone()));
                resource_requests.insert(gpu_resource_key.to_string(), Quantity(gpu_str));
            }
        }

        // Compute node rank from task_offset.
        // Issue #69: peer_nodes contains addr:port strings (e.g., "10.0.0.1:6818")
        // while target_node is a hostname — starts_with matching never worked,
        // causing all pods to get rank 0. Instead, derive rank from task_offset
        // which is incremented per-node by the dispatcher.
        let tasks_per_node = spec.tasks_per_node.max(1);
        let node_rank = req.task_offset / tasks_per_node;

        // Build env vars via SpurEnv accumulator
        let mut senv = SpurEnv::new();
        senv.set_with_slurm_twin("SPUR_JOB_ID", job_id);
        senv.set_with_slurm_twin("SPUR_JOBID", job_id);
        senv.set_with_slurm_twin("SPUR_JOB_NAME", &spec.name);
        senv.set_with_slurm_twin("SPUR_JOB_PARTITION", &spec.partition);
        senv.set_with_slurm_twin("SPUR_JOB_ACCOUNT", &spec.account);
        senv.set_with_slurm_twin("SPUR_JOB_QOS", &spec.qos);
        senv.set_with_slurm_twin("SPUR_NNODES", num_peers);
        senv.set_with_slurm_twin("SPUR_JOB_NUM_NODES", num_peers);
        senv.set_with_slurm_twin("SPUR_NTASKS", spec.num_tasks);
        senv.set_with_slurm_twin("SPUR_NPROCS", spec.num_tasks);
        senv.set_with_slurm_twin("SPUR_CPUS_PER_TASK", spec.cpus_per_task);
        senv.set_with_slurm_twin(
            "SPUR_CPUS_ON_NODE",
            tasks_per_node * spec.cpus_per_task.max(1),
        );
        senv.set_with_slurm_twin("SPUR_TASKS_PER_NODE", tasks_per_node);
        senv.set_with_slurm_twin("SPUR_NODEID", node_rank);
        senv.set_with_slurm_twin("SPUR_NODELIST", &spec.nodelist);
        senv.set_with_slurm_twin("SPUR_JOB_NODELIST", &spec.nodelist);
        senv.set_with_slurm_twin("SPURD_NODENAME", &target_node);

        senv.set("SPUR_TASK_OFFSET", req.task_offset);
        senv.set("SPUR_NODE_RANK", node_rank);
        if !peer_nodes.is_empty() {
            senv.set("SPUR_PEER_NODES", peer_nodes.join(","));
        }
        if !target_node.is_empty() {
            senv.set("SPUR_TARGET_NODE", &target_node);
        }

        senv.set("LOCAL_RANK", "0");
        senv.set("LOCAL_WORLD_SIZE", tasks_per_node);
        senv.set("NPROC_PER_NODE", tasks_per_node);
        senv.set("NODE_RANK", node_rank);

        if num_peers > 1 {
            let master_addr = execution_rank_zero_dns_name(&identity, &ns);
            senv.set("MASTER_ADDR", master_addr);
            senv.set("MASTER_PORT", "29500");
            senv.set("WORLD_SIZE", num_peers);
            senv.set("RANK", node_rank);
        }

        let mut env_vars: Vec<EnvVar> = senv
            .into_map()
            .into_iter()
            .map(|(name, value)| EnvVar {
                name,
                value: Some(value),
                ..Default::default()
            })
            .collect();

        // Set GPU vendor-specific env vars for the runtime
        let gpu_count = req
            .allocated
            .as_ref()
            .and_then(|a| a.devices.get("gpu"))
            .map(|d| d.devices.len())
            .unwrap_or(0);
        if gpu_count > 0 {
            let gpu_type = spec
                .gres
                .iter()
                .find_map(|g| spur_core::resource::parse_gres(g))
                .and_then(|(_, t, _)| t);
            if gpu_type.as_deref().is_none_or(|t| !is_nvidia_gpu(t)) {
                env_vars.push(EnvVar {
                    name: "GPU_ENABLE_PAL".into(),
                    value: Some("0".into()),
                    ..Default::default()
                });
                if num_peers > 1 {
                    env_vars.push(EnvVar {
                        name: "NCCL_SOCKET_IFNAME".into(),
                        value: Some("eth0".into()),
                        ..Default::default()
                    });
                }
            } else if num_peers > 1 {
                env_vars.push(EnvVar {
                    name: "NCCL_SOCKET_IFNAME".into(),
                    value: Some("eth0".into()),
                    ..Default::default()
                });
            }
        }

        for (k, v) in &spec.environment {
            env_vars.push(EnvVar {
                name: k.clone(),
                value: Some(v.clone()),
                ..Default::default()
            });
        }

        // Issue #117: Inject secret env vars from SpurJob CRD's secretEnv field.
        // These reference K8s Secrets and are injected as secretKeyRef, keeping
        // secret values out of the SpurJob spec and Raft log.
        {
            for (env_name, secret_ref) in &spur_job.spec.secret_env {
                if let Some((secret_name, secret_key)) = secret_ref.split_once('/') {
                    env_vars.push(EnvVar {
                        name: env_name.clone(),
                        value_from: Some(k8s_openapi::api::core::v1::EnvVarSource {
                            secret_key_ref: Some(k8s_openapi::api::core::v1::SecretKeySelector {
                                name: secret_name.to_string(),
                                key: secret_key.to_string(),
                                optional: Some(true),
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    });
                }
            }
        }
        // API create retries compare the exact workload contract. Stable sort
        // after every HashMap-derived source has been appended so equivalent
        // specs cannot differ only by randomized map iteration order. Rust's
        // stable sort preserves the fixed source precedence for duplicate names.
        env_vars.sort_by(|left, right| left.name.cmp(&right.name));

        // Build command
        let command = if !spec.argv.is_empty() {
            Some(spec.argv.clone())
        } else if !spec.script.is_empty() {
            Some(vec!["sh".into(), "-c".into(), spec.script.clone()])
        } else {
            // Interactive session: keep pod alive so kube exec can attach a terminal
            Some(vec!["sleep".into(), "infinity".into()])
        };

        // Parse container_mounts → volumes + volume_mounts
        let (mut volumes, mut volume_mounts) = parse_mounts(&spec.container_mounts);

        // Set working_dir from work_dir or container_workdir
        let working_dir = if !spec.container_workdir.is_empty() {
            Some(spec.container_workdir.clone())
        } else if !spec.work_dir.is_empty() {
            Some(spec.work_dir.clone())
        } else {
            None
        };

        // Add extra device plugin resources (RDMA, MIG, etc.) — Issue #88
        for (key, val) in &spec.extra_resources {
            resource_requests.insert(key.clone(), Quantity(val.clone()));
            resource_limits.insert(key.clone(), Quantity(val.clone()));
        }

        // Shared memory volume mount — Issue #87
        if !spec.shm_size.is_empty() {
            volume_mounts.push(k8s_openapi::api::core::v1::VolumeMount {
                name: "dshm".into(),
                mount_path: "/dev/shm".into(),
                ..Default::default()
            });
        }

        // Privileged mode / SecurityContext — Issue #86
        let security_context = if spec.privileged {
            Some(k8s_openapi::api::core::v1::SecurityContext {
                privileged: Some(true),
                ..Default::default()
            })
        } else {
            None
        };

        let container = Container {
            name: "spur-job".into(),
            image: Some(image),
            image_pull_policy: Some("IfNotPresent".into()),
            command,
            env: Some(env_vars),
            working_dir,
            volume_mounts: if volume_mounts.is_empty() {
                None
            } else {
                Some(volume_mounts)
            },
            resources: Some(ResourceRequirements {
                requests: Some(resource_requests),
                limits: Some(resource_limits),
                ..Default::default()
            }),
            security_context,
            termination_message_path: Some("/dev/termination-log".into()),
            termination_message_policy: Some("File".into()),
            ..Default::default()
        };

        // Build labels
        let mut labels = identity.execution_labels();
        labels.insert(
            "spur.amd.com/managed-by".to_string(),
            "spur-k8s-operator".to_string(),
        );
        if !spec.name.is_empty() {
            let value = sanitize_k8s_label_value(&spec.name);
            if !value.is_empty() {
                labels.insert("spur.amd.com/job-name".to_string(), value);
            }
        }
        if !target_node.is_empty() {
            let value = sanitize_k8s_label_value(&target_node);
            if !value.is_empty() {
                labels.insert("spur.amd.com/target-node".to_string(), value);
            }
        }

        // For multi-node jobs, create an execution-unique headless Service for
        // DNS discovery. A natural completion may leave an older attempt's
        // Service behind, so numeric job ID alone cannot be the DNS identity.
        if headless_service_name.is_some() {
            if let Err(e) = self
                .ensure_headless_service(&spur_job, &identity, &labels, &ns, &owner_reference)
                .await
            {
                warn!(job_id, error = %e, "failed to create headless service");
                return Err(e);
            }
        }

        // Pin to target K8s node
        let node_name = if !target_node.is_empty() {
            Some(target_node.clone())
        } else {
            peer_nodes.first().cloned()
        };

        // For headless service DNS: set hostname and subdomain
        let (hostname, subdomain) = if num_peers > 1 && !target_node.is_empty() {
            (
                Some(execution_pod_hostname(node_rank)),
                headless_service_name,
            )
        } else {
            (None, None)
        };

        let pod = Pod {
            metadata: ObjectMeta {
                name: Some(pod_name.clone()),
                namespace: Some(ns.clone()),
                labels: Some(labels),
                annotations: Some(identity.annotations()),
                owner_references: Some(vec![owner_reference.clone()]),
                ..Default::default()
            },
            spec: Some({
                // Shared memory emptyDir volume — Issue #87
                if !spec.shm_size.is_empty() {
                    volumes.push(k8s_openapi::api::core::v1::Volume {
                        name: "dshm".into(),
                        empty_dir: Some(k8s_openapi::api::core::v1::EmptyDirVolumeSource {
                            medium: Some("Memory".into()),
                            size_limit: Some(Quantity(spec.shm_size.clone())),
                        }),
                        ..Default::default()
                    });
                }

                PodSpec {
                    containers: vec![container],
                    automount_service_account_token: Some(false),
                    restart_policy: Some("Never".into()),
                    scheduler_name: Some("default-scheduler".into()),
                    service_account: Some("default".into()),
                    service_account_name: Some("default".into()),
                    preemption_policy: Some("PreemptLowerPriority".into()),
                    termination_grace_period_seconds: Some(30),
                    enable_service_links: Some(false),
                    node_name,
                    hostname,
                    subdomain,
                    volumes: if volumes.is_empty() {
                        None
                    } else {
                        Some(volumes)
                    },
                    // Issue #85: host_network
                    host_network: if spec.host_network { Some(true) } else { None },
                    dns_policy: Some(if spec.host_network {
                        "ClusterFirstWithHostNet".to_string()
                    } else {
                        "ClusterFirst".to_string()
                    }),
                    // Issue #87: host_ipc
                    host_ipc: if spec.host_ipc { Some(true) } else { None },
                    ..Default::default()
                }
            }),
            ..Default::default()
        };

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);
        match pods.create(&PostParams::default(), &pod).await {
            Ok(created) => {
                if !pod_matches_desired(&created, &pod) {
                    return Err(Status::failed_precondition(
                        "created Pod was mutated away from the exact desired workload",
                    ));
                }
                self.record_pod_provenance(&spur_job, &created).await?;
                info!(job_id, pod = %pod_name, namespace = %ns, target = %req.target_node, "K8s Pod created");
                Ok(Response::new(LaunchJobResponse {
                    success: true,
                    error: String::new(),
                    ..Default::default()
                }))
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                let existing = pods.get(&pod_name).await.map_err(|error| {
                    Status::aborted(format!(
                        "Pod {ns}/{pod_name} conflicted but could not be read: {error}"
                    ))
                })?;
                if identity.matches_pod(&existing)
                    && resource_has_controller_owner(&existing.metadata, &owner_reference.uid)
                    && pod_matches_desired(&existing, &pod)
                {
                    self.record_pod_provenance(&spur_job, &existing).await?;
                    info!(job_id, pod = %pod_name, namespace = %ns, target = %req.target_node, "exact K8s Pod already exists, treating dispatch as idempotent");
                    Ok(Response::new(LaunchJobResponse {
                        success: true,
                        error: String::new(),
                        ..Default::default()
                    }))
                } else {
                    Err(Status::failed_precondition(format!(
                        "Pod {ns}/{pod_name} belongs to a different execution"
                    )))
                }
            }
            Err(e) => {
                error!(job_id, error = %e, "failed to create K8s Pod");
                Ok(Response::new(LaunchJobResponse {
                    success: false,
                    error: e.to_string(),
                    ..Default::default()
                }))
            }
        }
    }

    async fn prepare_pmix(
        &self,
        _request: Request<PreparePmixRequest>,
    ) -> Result<Response<PreparePmixResponse>, Status> {
        Err(Status::unimplemented(
            "PMIx prepare is not supported on the K8s virtual agent",
        ))
    }

    async fn release_pmix(
        &self,
        request: Request<ReleasePmixRequest>,
    ) -> Result<Response<ReleasePmixResponse>, Status> {
        let req = request.into_inner();
        let identity = PodExecutionIdentity::from_request(
            req.job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
        )?;
        self.require_current_identity(&identity).await?;
        Ok(Response::new(ReleasePmixResponse {}))
    }

    async fn cancel_job(
        &self,
        request: Request<AgentCancelJobRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let identity = PodExecutionIdentity::from_request(
            req.job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
        )?;
        let mode = AgentJobControlMode::try_from(req.control_mode)
            .map_err(|_| Status::invalid_argument("unknown job control mode"))?;
        match mode {
            AgentJobControlMode::AgentJobControlTerminateAndReap => {}
            AgentJobControlMode::AgentJobControlSignalOnly => {
                return Err(Status::unimplemented(
                    "signal-only job control is not supported by the K8s virtual agent",
                ));
            }
            AgentJobControlMode::AgentJobControlUnspecified => {
                return Err(Status::invalid_argument("job control mode is required"));
            }
        }

        self.terminate_exact_execution(&identity).await?;

        Ok(Response::new(()))
    }

    async fn suspend_job(
        &self,
        request: Request<AgentSuspendJobRequest>,
    ) -> Result<Response<()>, Status> {
        let req = request.into_inner();
        let identity = PodExecutionIdentity::from_request(
            req.job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
        )?;
        self.find_exact_pod(&identity).await?;
        // Pod-level SIGSTOP/SIGCONT is not modeled for the k8s backend. The
        // exact identity gate still makes this no-op idempotent and prevents a
        // stale control RPC from acknowledging against a replacement Pod.
        debug!(
            job_id = req.job_id,
            resume = req.resume,
            "k8s backend: suspend/resume is a no-op"
        );
        Ok(Response::new(()))
    }

    async fn get_node_resources(
        &self,
        _request: Request<()>,
    ) -> Result<Response<NodeResourcesResponse>, Status> {
        Ok(Response::new(NodeResourcesResponse {
            total: Some(ResourceSet::default()),
            used: Some(spur_proto::proto::ResourceAllocations::default()),
        }))
    }

    async fn exec_in_job(
        &self,
        request: Request<ExecInJobRequest>,
    ) -> Result<Response<ExecInJobResponse>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let identity = PodExecutionIdentity::from_request(
            job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
        )?;
        let pod = self.find_exact_pod(&identity).await?;
        let ns = pod
            .metadata
            .namespace
            .ok_or_else(|| Status::internal("exact Pod has no namespace"))?;
        let pod_name = pod
            .metadata
            .name
            .ok_or_else(|| Status::internal("exact Pod has no name"))?;
        let command: Vec<String> = if req.command.is_empty() {
            vec!["bash".into(), "-c".into(), "echo ok".into()]
        } else {
            req.command
        };

        debug!(pod = %pod_name, cmd = ?command, "exec in K8s pod");

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);

        let attach = AttachParams {
            stdin: false,
            stdout: true,
            stderr: true,
            tty: false,
            container: None,
            max_stdin_buf_size: None,
            max_stdout_buf_size: Some(1024 * 1024),
            max_stderr_buf_size: Some(1024 * 1024),
        };

        let mut exec = pods
            .exec(&pod_name, command, &attach)
            .await
            .map_err(|e| Status::internal(format!("exec failed: {e}")))?;

        let mut stdout_data = Vec::new();
        let mut stderr_data = Vec::new();

        if let Some(mut stdout) = exec.stdout() {
            let _ = stdout.read_to_end(&mut stdout_data).await;
        }
        if let Some(mut stderr) = exec.stderr() {
            let _ = stderr.read_to_end(&mut stderr_data).await;
        }

        let status = exec
            .take_status()
            .ok_or_else(|| Status::internal("no exit status"))?
            .await
            .ok_or_else(|| Status::internal("status channel closed"))?;

        let exit_code = status
            .status
            .as_deref()
            .map(|s| if s == "Success" { 0 } else { 1 })
            .unwrap_or(1);

        Ok(Response::new(ExecInJobResponse {
            success: exit_code == 0,
            exit_code,
            stdout: String::from_utf8_lossy(&stdout_data).into_owned(),
            stderr: String::from_utf8_lossy(&stderr_data).into_owned(),
        }))
    }

    async fn run_command(
        &self,
        _request: Request<RunCommandRequest>,
    ) -> Result<Response<RunCommandResponse>, Status> {
        // Srun step dispatch. The K8s virtual agent does not currently support
        // one-shot commands outside the job pod's lifecycle — salloc plus
        // srun-in-allocation is not a common K8s workflow.
        Err(Status::unimplemented(
            "RunCommand is not yet supported by the K8s virtual agent",
        ))
    }

    async fn cancel_step(
        &self,
        _request: Request<CancelStepRequest>,
    ) -> Result<Response<()>, Status> {
        Err(Status::unimplemented(
            "CancelStep is not yet supported by the K8s virtual agent",
        ))
    }

    async fn register_job_allocation(
        &self,
        _request: Request<RegisterJobAllocationRequest>,
    ) -> Result<Response<RegisterJobAllocationResponse>, Status> {
        Err(Status::unimplemented(
            "RegisterJobAllocation is not yet supported by the K8s virtual agent",
        ))
    }

    async fn stream_job_output(
        &self,
        request: Request<StreamJobOutputRequest>,
    ) -> Result<Response<Self::StreamJobOutputStream>, Status> {
        let req = request.into_inner();
        let job_id = req.job_id;
        let identity = PodExecutionIdentity::from_request(
            job_id,
            &req.submission_generation,
            req.run_attempt,
            &req.worker_incarnation,
        )?;
        let pod = self.find_exact_pod(&identity).await?;
        let ns = pod
            .metadata
            .namespace
            .ok_or_else(|| Status::internal("exact Pod has no namespace"))?;
        let pod_name = pod
            .metadata
            .name
            .ok_or_else(|| Status::internal("exact Pod has no name"))?;

        debug!(pod = %pod_name, "streaming logs from K8s pod");

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);
        let log_params = kube::api::LogParams {
            follow: true,
            tail_lines: Some(100),
            ..Default::default()
        };

        let log_stream = pods
            .log_stream(&pod_name, &log_params)
            .await
            .map_err(|e| Status::internal(format!("log stream failed: {e}")))?;

        let (tx, rx) = tokio::sync::mpsc::channel(64);

        tokio::spawn(async move {
            use futures_util::AsyncReadExt;
            let mut reader = log_stream;
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if tx
                            .send(Ok(StreamJobOutputChunk {
                                data: buf[..n].to_vec(),
                                eof: false,
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let _ = tx
                .send(Ok(StreamJobOutputChunk {
                    data: Vec::new(),
                    eof: true,
                }))
                .await;
        });

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }

    async fn interactive_session(
        &self,
        _request: Request<tonic::Streaming<InteractiveInput>>,
    ) -> Result<Response<Self::InteractiveSessionStream>, Status> {
        Err(Status::unimplemented(
            "interactive session not supported for K8s agent",
        ))
    }

    // -- Native cluster component control. The virtual K8s agent does not run k0s
    //    systemd units, so these are permanently unsupported here. --
    async fn start_cluster_component(
        &self,
        _request: Request<StartClusterComponentRequest>,
    ) -> Result<Response<StartClusterComponentResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn stop_cluster_component(
        &self,
        _request: Request<StopClusterComponentRequest>,
    ) -> Result<Response<StopClusterComponentResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn get_cluster_component_status(
        &self,
        _request: Request<GetClusterComponentStatusRequest>,
    ) -> Result<Response<GetClusterComponentStatusResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn create_k0s_join_token(
        &self,
        _request: Request<CreateK0sJoinTokenRequest>,
    ) -> Result<Response<CreateK0sJoinTokenResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn get_kubeconfig(
        &self,
        _request: Request<GetKubeconfigRequest>,
    ) -> Result<Response<GetKubeconfigResponse>, Status> {
        Err(Status::unimplemented(
            "cluster components not supported for K8s agent",
        ))
    }

    async fn apply_mesh(
        &self,
        _request: Request<MeshMembership>,
    ) -> Result<Response<ApplyMeshResponse>, Status> {
        Err(Status::unimplemented("mesh not supported for K8s agent"))
    }
}

impl VirtualAgent {
    /// Delete only resources owned by this exact execution and acknowledge
    /// only after its Pod is absent. A delayed cleanup for a reused numeric job
    /// ID therefore cannot delete a replacement Pod or Service.
    async fn terminate_exact_execution(
        &self,
        identity: &PodExecutionIdentity,
    ) -> Result<(), Status> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let exact_pods = self.list_exact_pods(identity, true).await?;
                if exact_pods.is_empty() {
                    break Ok::<(), Status>(());
                }
                for pod in exact_pods {
                    let namespace = pod
                        .metadata
                        .namespace
                        .as_deref()
                        .ok_or_else(|| Status::internal("exact Pod has no namespace"))?;
                    let name = pod
                        .metadata
                        .name
                        .as_deref()
                        .ok_or_else(|| Status::internal("exact Pod has no name"))?;
                    let uid = pod
                        .metadata
                        .uid
                        .clone()
                        .ok_or_else(|| Status::internal("exact Pod has no UID"))?;
                    let api: Api<Pod> = Api::namespaced(self.client.clone(), namespace);
                    let delete_params = DeleteParams {
                        grace_period_seconds: Some(0),
                        preconditions: Some(Preconditions {
                            uid: Some(uid.clone()),
                            resource_version: None,
                        }),
                        ..Default::default()
                    };
                    match api.delete(name, &delete_params).await {
                        Ok(_) => info!(job_id = identity.key.job_id, pod = %name, %uid, "deleting exact Pod UID"),
                        Err(kube::Error::Api(error))
                            if error.code == 404 || error.code == 409 =>
                        {
                            // The exact UID is already absent or the name now
                            // denotes another object. Re-list before acting.
                        }
                        Err(error) => {
                            return Err(Status::internal(format!(
                                "failed to delete exact Pod {namespace}/{name} UID {uid}: {error}"
                            )));
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "timed out waiting for exact job {} Pods to terminate",
                identity.key.job_id
            ))
        })??;

        let lp = ListParams::default().labels(&format!("{JOB_ID_LABEL}={}", identity.key.job_id));
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let owner = self.resolve_spur_job(identity).await?;
                let exact_identity = Self::hydrate_control_identity(&owner, identity)?;
                let namespace = owner.metadata.namespace.as_deref().ok_or_else(|| {
                    Status::failed_precondition("owning SpurJob has no namespace")
                })?;
                let owner_uid = owner
                    .metadata
                    .uid
                    .as_deref()
                    .ok_or_else(|| Status::failed_precondition("owning SpurJob has no UID"))?;
                let status = owner.status.as_ref().ok_or_else(|| {
                    Status::failed_precondition("owning SpurJob has no durable status")
                })?;
                let services: Api<Service> =
                    Api::namespaced(self.client.clone(), namespace);
                let listed = services
                    .list(&lp)
                    .await
                    .map_err(|error| {
                        Status::internal(format!("failed to list job Services: {error}"))
                    })?;
                let matching_services = classify_control_services(
                    status,
                    owner_uid,
                    &exact_identity,
                    listed.items,
                    true,
                )
                .map_err(Status::failed_precondition)?;
                if matching_services.is_empty() {
                    return Ok::<(), Status>(());
                }
                for service in matching_services {
                    let namespace = service
                        .metadata
                        .namespace
                        .as_deref()
                        .ok_or_else(|| Status::internal("exact Service has no namespace"))?;
                    let name = service
                        .metadata
                        .name
                        .as_deref()
                        .ok_or_else(|| Status::internal("exact Service has no name"))?;
                    let uid = service
                        .metadata
                        .uid
                        .clone()
                        .ok_or_else(|| Status::internal("exact Service has no UID"))?;
                    let delete_params = DeleteParams {
                        preconditions: Some(Preconditions {
                            uid: Some(uid),
                            resource_version: None,
                        }),
                        ..Default::default()
                    };
                    match services.delete(name, &delete_params).await {
                        Ok(_) => {
                            debug!(job_id = identity.key.job_id, service = %name, "deleting exact headless Service")
                        }
                        Err(kube::Error::Api(error))
                            if error.code == 404 || error.code == 409 => {}
                        Err(error) => {
                            return Err(Status::internal(format!(
                                "failed to delete exact Service {namespace}/{name}: {error}"
                            )));
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .map_err(|_| {
            Status::deadline_exceeded(format!(
                "timed out waiting for exact job {} Service to terminate",
                identity.key.job_id
            ))
        })??;

        Ok(())
    }

    /// Create a headless Service for inter-pod DNS discovery in multi-node jobs.
    async fn ensure_headless_service(
        &self,
        owner: &SpurJob,
        identity: &PodExecutionIdentity,
        labels: &BTreeMap<String, String>,
        namespace: &str,
        owner_reference: &OwnerReference,
    ) -> Result<(), Status> {
        let services: Api<Service> = Api::namespaced(self.client.clone(), namespace);
        let job_id = identity.key.job_id;
        let svc_name = execution_service_name(identity);
        let service_dispatch_token = self
            .ensure_service_dispatch_token(owner, identity, &svc_name)
            .await?;

        let selector = BTreeMap::from([
            (JOB_ID_LABEL.to_string(), job_id.to_string()),
            (
                SUBMISSION_GENERATION_LABEL.to_string(),
                identity.key.submission_generation.clone(),
            ),
            (
                RUN_ATTEMPT_LABEL.to_string(),
                identity.key.run_attempt.to_string(),
            ),
        ]);

        let mut service_labels = identity.execution_labels();
        service_labels.insert(
            "spur.amd.com/managed-by".to_string(),
            "spur-k8s-operator".to_string(),
        );
        if let Some(job_name) = labels.get("spur.amd.com/job-name") {
            service_labels.insert("spur.amd.com/job-name".to_string(), job_name.clone());
        }
        let svc = Service {
            metadata: ObjectMeta {
                name: Some(svc_name.clone()),
                namespace: Some(namespace.to_string()),
                labels: Some(service_labels),
                annotations: Some(identity.service_annotations(&service_dispatch_token)),
                owner_references: Some(vec![owner_reference.clone()]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".into()), // headless
                publish_not_ready_addresses: Some(true),
                type_: Some("ClusterIP".into()),
                session_affinity: Some("None".into()),
                internal_traffic_policy: Some("Cluster".into()),
                selector: Some(selector),
                ports: Some(vec![ServicePort {
                    name: Some("nccl".into()),
                    port: 29500,
                    target_port: Some(IntOrString::Int(29500)),
                    protocol: Some("TCP".into()),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let exact_service = match services.create(&PostParams::default(), &svc).await {
            Ok(created) => {
                if !service_matches_desired(&created, &svc) {
                    return Err(Status::failed_precondition(
                        "created Service was mutated away from the exact desired contract",
                    ));
                }
                info!(job_id, svc = %svc_name, "headless Service created");
                created
            }
            Err(kube::Error::Api(e)) if e.code == 409 => {
                let existing = services.get(&svc_name).await.map_err(|error| {
                    Status::aborted(format!(
                        "Service {namespace}/{svc_name} conflicted but could not be read: {error}"
                    ))
                })?;
                if identity.owns_service(&existing)
                    && resource_has_controller_owner(&existing.metadata, &owner_reference.uid)
                    && existing
                        .metadata
                        .annotations
                        .as_ref()
                        .and_then(|annotations| annotations.get(SERVICE_DISPATCH_TOKEN_ANNOTATION))
                        == Some(&service_dispatch_token)
                    && service_matches_desired(&existing, &svc)
                {
                    debug!(job_id, "exact headless Service already exists");
                    existing
                } else {
                    return Err(Status::failed_precondition(format!(
                        "Service {namespace}/{svc_name} belongs to a different execution"
                    )));
                }
            }
            Err(error) => {
                return Err(Status::internal(format!(
                    "failed to create Service {namespace}/{svc_name}: {error}"
                )));
            }
        };
        self.record_service_provenance(owner, identity, &exact_service)
            .await
    }
}

fn metadata_contains_desired(actual: &ObjectMeta, desired: &ObjectMeta) -> bool {
    let mut actual_annotations = actual.annotations.clone().unwrap_or_default();
    if actual_annotations
        .get(PROVENANCE_RECORDED_ANNOTATION)
        .map(String::as_str)
        == Some("true")
    {
        actual_annotations.remove(PROVENANCE_RECORDED_ANNOTATION);
    }
    actual.labels.clone().unwrap_or_default() == desired.labels.clone().unwrap_or_default()
        && actual_annotations == desired.annotations.clone().unwrap_or_default()
        && actual.owner_references == desired.owner_references
        && actual.finalizers.as_ref().is_none_or(Vec::is_empty)
}

fn pod_matches_desired(actual: &Pod, desired: &Pod) -> bool {
    if actual.metadata.name != desired.metadata.name
        || actual.metadata.namespace != desired.metadata.namespace
        || !metadata_contains_desired(&actual.metadata, &desired.metadata)
    {
        return false;
    }
    let (Some(actual), Some(desired)) = (actual.spec.as_ref(), desired.spec.as_ref()) else {
        return false;
    };
    normalized_pod_spec(actual) == normalized_pod_spec(desired)
}

fn normalized_pod_spec(spec: &PodSpec) -> PodSpec {
    let mut normalized = spec.clone();
    if normalized.priority == Some(0) {
        normalized.priority = None;
    }
    if normalized.security_context.as_ref()
        == Some(&k8s_openapi::api::core::v1::PodSecurityContext::default())
    {
        normalized.security_context = None;
    }
    if let Some(tolerations) = normalized.tolerations.as_mut() {
        tolerations.retain(|toleration| {
            let is_default_noexecute = matches!(
                toleration.key.as_deref(),
                Some("node.kubernetes.io/not-ready" | "node.kubernetes.io/unreachable")
            ) && toleration.operator.as_deref() == Some("Exists")
                && toleration.effect.as_deref() == Some("NoExecute")
                && toleration.value.is_none()
                && toleration.toleration_seconds == Some(300);
            !is_default_noexecute
        });
        if tolerations.is_empty() {
            normalized.tolerations = None;
        }
    }
    normalize_pod_quantities(&mut normalized);
    normalized
}

fn normalize_pod_quantities(spec: &mut PodSpec) {
    for container in &mut spec.containers {
        if let Some(resources) = container.resources.as_mut() {
            for quantities in [&mut resources.requests, &mut resources.limits]
                .into_iter()
                .flatten()
            {
                for quantity in quantities.values_mut() {
                    if let Some(normalized) = normalized_quantity(&quantity.0) {
                        quantity.0 = normalized;
                    }
                }
            }
        }
    }
    if let Some(volumes) = spec.volumes.as_mut() {
        for volume in volumes {
            if let Some(quantity) = volume
                .empty_dir
                .as_mut()
                .and_then(|empty_dir| empty_dir.size_limit.as_mut())
            {
                if let Some(normalized) = normalized_quantity(&quantity.0) {
                    quantity.0 = normalized;
                }
            }
        }
    }
}

/// Convert the Kubernetes quantity forms used by this operator to one exact
/// rational representation. API-server canonicalization can rewrite `1024Mi`
/// to `1Gi` or decimal CPU forms without changing their value; adoption must
/// compare semantics, not the original spelling.
fn normalized_quantity(input: &str) -> Option<String> {
    fn pow10(exponent: u32) -> Option<i128> {
        (0..exponent).try_fold(1i128, |value, _| value.checked_mul(10))
    }
    fn gcd(mut left: i128, mut right: i128) -> i128 {
        left = left.abs();
        right = right.abs();
        while right != 0 {
            let remainder = left % right;
            left = right;
            right = remainder;
        }
        left.max(1)
    }

    let (number, multiplier, divisor) = [
        ("Ki", 1i128 << 10, 1i128),
        ("Mi", 1i128 << 20, 1i128),
        ("Gi", 1i128 << 30, 1i128),
        ("Ti", 1i128 << 40, 1i128),
        ("Pi", 1i128 << 50, 1i128),
        ("Ei", 1i128 << 60, 1i128),
        ("n", 1, 1_000_000_000),
        ("u", 1, 1_000_000),
        ("m", 1, 1_000),
        ("k", 1_000, 1),
        ("K", 1_000, 1),
        ("M", 1_000_000, 1),
        ("G", 1_000_000_000, 1),
        ("T", 1_000_000_000_000, 1),
        ("P", 1_000_000_000_000_000, 1),
        ("E", 1_000_000_000_000_000_000, 1),
    ]
    .into_iter()
    .find_map(|(suffix, multiplier, divisor)| {
        input
            .strip_suffix(suffix)
            .filter(|number| !number.is_empty())
            .map(|number| (number, multiplier, divisor))
    })
    .unwrap_or((input, 1, 1));

    let (mantissa, exponent) = if let Some(index) = number.find(['e', 'E']) {
        (&number[..index], number[index + 1..].parse::<i32>().ok()?)
    } else {
        (number, 0i32)
    };
    let negative = mantissa.starts_with('-');
    let unsigned = mantissa
        .strip_prefix('-')
        .or_else(|| mantissa.strip_prefix('+'))
        .unwrap_or(mantissa);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty() && fraction.is_empty() {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let mut numerator = digits.parse::<i128>().ok()?;
    if negative {
        numerator = -numerator;
    }
    let mut denominator = pow10(fraction.len() as u32)?;
    numerator = numerator.checked_mul(multiplier)?;
    denominator = denominator.checked_mul(divisor)?;
    if exponent >= 0 {
        numerator = numerator.checked_mul(pow10(exponent as u32)?)?;
    } else {
        denominator = denominator.checked_mul(pow10(exponent.unsigned_abs())?)?;
    }
    let common = gcd(numerator, denominator);
    Some(format!("{}/{}", numerator / common, denominator / common))
}

fn service_matches_desired(actual: &Service, desired: &Service) -> bool {
    if actual.metadata.name != desired.metadata.name
        || actual.metadata.namespace != desired.metadata.namespace
        || !metadata_contains_desired(&actual.metadata, &desired.metadata)
    {
        return false;
    }
    let (Some(actual), Some(desired)) = (actual.spec.as_ref(), desired.spec.as_ref()) else {
        return false;
    };
    if actual.cluster_ip.as_deref() != Some("None") {
        return false;
    }
    let mut actual = actual.clone();
    let mut desired = desired.clone();
    // These are assigned/defaulted by the API server for a headless Service;
    // they do not change selection, exposure, or workload routing semantics.
    actual.cluster_ips = None;
    actual.ip_families = None;
    actual.ip_family_policy = None;
    desired.cluster_ips = None;
    desired.ip_families = None;
    desired.ip_family_policy = None;
    actual == desired
}

/// Parse container_mounts ("/src:/dst:ro" or "pvc:name:/dst") into K8s volumes + mounts.
fn parse_mounts(mounts: &[String]) -> (Vec<Volume>, Vec<VolumeMount>) {
    let mut volumes = Vec::new();
    let mut volume_mounts = Vec::new();

    for (i, mount_str) in mounts.iter().enumerate() {
        let parts: Vec<&str> = mount_str.split(':').collect();

        if parts.len() >= 2 && parts[0] == "pvc" {
            // PVC mount: "pvc:claim-name:/dst"
            if parts.len() >= 3 {
                let vol_name = format!("pvc-{}", i);
                volumes.push(Volume {
                    name: vol_name.clone(),
                    persistent_volume_claim: Some(
                        k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource {
                            claim_name: parts[1].to_string(),
                            read_only: Some(parts.get(3).is_some_and(|&v| v == "ro")),
                        },
                    ),
                    ..Default::default()
                });
                volume_mounts.push(VolumeMount {
                    name: vol_name,
                    mount_path: parts[2].to_string(),
                    read_only: Some(parts.get(3).is_some_and(|&v| v == "ro")),
                    ..Default::default()
                });
            }
        } else if parts.len() >= 2 {
            // hostPath mount: "/src:/dst[:ro]"
            let vol_name = format!("hostpath-{}", i);
            let read_only = parts.get(2).is_some_and(|&v| v == "ro");
            volumes.push(Volume {
                name: vol_name.clone(),
                host_path: Some(HostPathVolumeSource {
                    path: parts[0].to_string(),
                    type_: Some("DirectoryOrCreate".into()),
                }),
                ..Default::default()
            });
            volume_mounts.push(VolumeMount {
                name: vol_name,
                mount_path: parts[1].to_string(),
                read_only: Some(read_only),
                ..Default::default()
            });
        }
    }

    (volumes, volume_mounts)
}

/// Sanitize a string for use in K8s resource names.
fn sanitize_k8s_name(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

fn sanitize_k8s_label_value(value: &str) -> String {
    let sanitized = sanitize_k8s_name(value);
    sanitized[..sanitized.len().min(63)]
        .trim_end_matches('-')
        .to_string()
}

/// Stable, execution-unique Pod name. Keep the identity digest intact and
/// truncate only the human-readable node suffix to Kubernetes' 253-byte DNS
/// subdomain limit.
fn execution_pod_name(identity: &PodExecutionIdentity, target_node: &str) -> String {
    // FNV-1a is intentionally implemented here rather than using
    // `DefaultHasher`, whose algorithm is not a cross-version API contract.
    // The same execution must retain its name after an operator/compiler
    // upgrade so dispatch retries remain idempotent.
    let mut digest = 0xcbf29ce484222325u64;
    let mut update = |part: &[u8]| {
        for byte in (part.len() as u64).to_le_bytes().iter().chain(part) {
            digest ^= u64::from(*byte);
            digest = digest.wrapping_mul(0x100000001b3);
        }
    };
    update(&identity.key.job_id.to_le_bytes());
    update(identity.key.submission_generation.as_bytes());
    update(&identity.key.run_attempt.to_le_bytes());
    update(identity.worker_incarnation.as_bytes());
    let prefix = format!("spur-job-{}-{:016x}", identity.key.job_id, digest);
    let node = sanitize_k8s_name(target_node);
    if node.is_empty() {
        return prefix;
    }
    let max_node_len = 253usize.saturating_sub(prefix.len() + 1);
    let node = &node[..node.len().min(max_node_len)];
    format!("{prefix}-{node}").trim_end_matches('-').to_string()
}

/// Stable DNS-label name shared by all peers of one exact run attempt.
/// Worker incarnation is intentionally excluded because every worker in the
/// multi-node execution must use the same headless Service.
fn execution_service_name(identity: &PodExecutionIdentity) -> String {
    let mut digest = 0xcbf29ce484222325u64;
    let mut update = |part: &[u8]| {
        for byte in (part.len() as u64).to_le_bytes().iter().chain(part) {
            digest ^= u64::from(*byte);
            digest = digest.wrapping_mul(0x100000001b3);
        }
    };
    update(&identity.key.job_id.to_le_bytes());
    update(identity.key.submission_generation.as_bytes());
    update(&identity.key.run_attempt.to_le_bytes());
    format!("spur-job-{}-{digest:016x}", identity.key.job_id)
}

fn execution_pod_hostname(node_rank: u32) -> String {
    format!("rank-{node_rank}")
}

fn execution_rank_zero_dns_name(identity: &PodExecutionIdentity, namespace: &str) -> String {
    format!(
        "{}.{}.{}.svc.cluster.local",
        execution_pod_hostname(0),
        execution_service_name(identity),
        namespace
    )
}

/// Determine the K8s device plugin resource key based on GPU type.
///
/// AMD GPUs (mi300x, mi250x, gfx*, etc.) → "amd.com/gpu"
/// NVIDIA GPUs (h100, a100, etc.) → "nvidia.com/gpu"
/// Unknown/generic → "amd.com/gpu" (AMD-first default for ROCm project)
fn gpu_vendor_resource_key(gpu_type: Option<&str>) -> &'static str {
    match gpu_type {
        Some(t) if is_nvidia_gpu(t) => "nvidia.com/gpu",
        _ => "amd.com/gpu",
    }
}

/// Check if a GPU type string refers to an NVIDIA GPU.
fn is_nvidia_gpu(gpu_type: &str) -> bool {
    let lower = gpu_type.to_lowercase();
    // NVIDIA product families
    lower.starts_with("h100")
        || lower.starts_with("h200")
        || lower.starts_with("a100")
        || lower.starts_with("a10g")
        || lower.starts_with("a30")
        || lower.starts_with("v100")
        || lower.starts_with("t4")
        || lower.starts_with("l4")
        || lower.starts_with("l40")
        || lower.starts_with("b100")
        || lower.starts_with("b200")
        || lower.starts_with("gb200")
        || lower.starts_with("rtx")
        || lower == "nvidia"
}

/// Build a gres string from GPU count and type.
pub fn gpu_request_to_gres(count: u32, gpu_type: Option<&str>) -> String {
    let t = gpu_type.unwrap_or("any");
    let t = if t.is_empty() { "any" } else { t };
    format!("gpu:{}:{}", t, count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_owner(uid: &str) -> OwnerReference {
        OwnerReference {
            api_version: "spur.amd.com/v1alpha1".into(),
            kind: "SpurJob".into(),
            name: "owner".into(),
            uid: uid.into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    // --- gpu_request_to_gres ---

    #[test]
    fn test_gpu_request_to_gres() {
        assert_eq!(gpu_request_to_gres(8, Some("mi300x")), "gpu:mi300x:8");
        assert_eq!(gpu_request_to_gres(4, None), "gpu:any:4");
        assert_eq!(gpu_request_to_gres(2, Some("")), "gpu:any:2");
    }

    #[test]
    fn test_gpu_request_to_gres_single() {
        assert_eq!(gpu_request_to_gres(1, Some("h100")), "gpu:h100:1");
    }

    // --- sanitize_k8s_name ---

    #[test]
    fn test_sanitize_k8s_name() {
        assert_eq!(sanitize_k8s_name("gpu-node-01"), "gpu-node-01");
        assert_eq!(sanitize_k8s_name("NODE_WITH.DOTS"), "node-with-dots");
        assert_eq!(sanitize_k8s_name("--leading--"), "leading");
    }

    #[test]
    fn test_sanitize_k8s_name_uppercase() {
        assert_eq!(sanitize_k8s_name("GPU-NODE-01"), "gpu-node-01");
    }

    #[test]
    fn test_sanitize_k8s_name_spaces_and_special() {
        assert_eq!(sanitize_k8s_name("my node@#$123"), "my-node---123");
    }

    #[test]
    fn test_sanitize_k8s_name_all_special() {
        assert_eq!(sanitize_k8s_name("@@@"), "");
    }

    #[test]
    fn test_sanitize_k8s_name_already_clean() {
        assert_eq!(sanitize_k8s_name("worker-3"), "worker-3");
    }

    #[test]
    fn execution_pod_name_is_stable_and_exact_identity_scoped() {
        let identity = PodExecutionIdentity::from_request(41, "generation-a", 2, "worker-a")
            .expect("identity");
        assert_eq!(
            execution_pod_name(&identity, "GPU-NODE-01"),
            execution_pod_name(&identity, "GPU-NODE-01")
        );

        for replacement in [
            PodExecutionIdentity::from_request(41, "generation-b", 2, "worker-a").unwrap(),
            PodExecutionIdentity::from_request(41, "generation-a", 3, "worker-a").unwrap(),
            PodExecutionIdentity::from_request(41, "generation-a", 2, "worker-b").unwrap(),
        ] {
            assert_ne!(
                execution_pod_name(&identity, "GPU-NODE-01"),
                execution_pod_name(&replacement, "GPU-NODE-01"),
                "a reused numeric job ID must have a distinct Pod name"
            );
        }
    }

    #[test]
    fn execution_pod_name_preserves_digest_with_long_node_name() {
        let identity = PodExecutionIdentity::from_request(41, "generation-a", 2, "worker-a")
            .expect("identity");
        let name = execution_pod_name(&identity, &"node".repeat(100));
        assert!(name.len() <= 253);
        assert!(name.starts_with("spur-job-41-"));
        assert!(!name.ends_with('-'));
    }

    #[test]
    fn multi_node_requeue_uses_attempt_unique_service_and_shared_peer_dns() {
        let first_worker = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            1,
            "worker-a",
            "submission-token-a",
        )
        .expect("first worker identity");
        let peer_worker = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            1,
            "worker-b",
            "submission-token-a",
        )
        .expect("peer worker identity");
        let second_attempt = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            2,
            "worker-a",
            "submission-token-a",
        )
        .expect("second attempt identity");

        assert_eq!(
            execution_service_name(&first_worker),
            execution_service_name(&peer_worker),
            "all peers in one attempt need the same DNS subdomain"
        );
        assert_ne!(
            execution_service_name(&first_worker),
            execution_service_name(&second_attempt),
            "a naturally completed attempt must not block a requeue Service"
        );
        assert!(execution_service_name(&second_attempt).len() <= 63);
        assert_eq!(
            execution_rank_zero_dns_name(&first_worker, "training"),
            format!(
                "rank-0.{}.training.svc.cluster.local",
                execution_service_name(&first_worker)
            )
        );
        assert_eq!(execution_pod_hostname(u32::MAX), "rank-4294967295");
    }

    #[test]
    fn kubernetes_label_values_are_bounded() {
        let value = sanitize_k8s_label_value(&"Very_Long.Node_Name".repeat(20));
        assert!(value.len() <= 63);
        assert!(!value.ends_with('-'));
    }

    #[test]
    fn quantity_normalization_compares_semantic_values() {
        assert_eq!(normalized_quantity("1024Mi"), normalized_quantity("1Gi"));
        assert_eq!(normalized_quantity("1000m"), normalized_quantity("1"));
        assert_eq!(normalized_quantity("1e3"), normalized_quantity("1k"));
        assert_ne!(normalized_quantity("1Gi"), normalized_quantity("1000Mi"));
        assert_eq!(normalized_quantity("not-a-quantity"), None);
    }

    #[test]
    fn pod_adoption_accepts_only_api_defaults_not_workload_drift() {
        let desired = Pod {
            metadata: ObjectMeta {
                name: Some("exact-pod".into()),
                namespace: Some("training".into()),
                labels: Some(BTreeMap::from([("app".into(), "spur".into())])),
                annotations: Some(BTreeMap::from([("nonce".into(), "one".into())])),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "job".into(),
                    image: Some("image:v1".into()),
                    resources: Some(ResourceRequirements {
                        requests: Some(BTreeMap::from([(
                            "memory".into(),
                            Quantity("1024Mi".into()),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                restart_policy: Some("Never".into()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let mut api_defaulted = desired.clone();
        let spec = api_defaulted.spec.as_mut().unwrap();
        spec.priority = Some(0);
        spec.security_context = Some(Default::default());
        spec.tolerations = Some(vec![
            k8s_openapi::api::core::v1::Toleration {
                key: Some("node.kubernetes.io/not-ready".into()),
                operator: Some("Exists".into()),
                effect: Some("NoExecute".into()),
                toleration_seconds: Some(300),
                ..Default::default()
            },
            k8s_openapi::api::core::v1::Toleration {
                key: Some("node.kubernetes.io/unreachable".into()),
                operator: Some("Exists".into()),
                effect: Some("NoExecute".into()),
                toleration_seconds: Some(300),
                ..Default::default()
            },
        ]);
        spec.containers[0]
            .resources
            .as_mut()
            .unwrap()
            .requests
            .as_mut()
            .unwrap()
            .insert("memory".into(), Quantity("1Gi".into()));
        assert!(pod_matches_desired(&api_defaulted, &desired));

        let mut changed_image = api_defaulted.clone();
        changed_image.spec.as_mut().unwrap().containers[0].image = Some("image:v2".into());
        assert!(!pod_matches_desired(&changed_image, &desired));

        let mut changed_isolation = api_defaulted.clone();
        changed_isolation.spec.as_mut().unwrap().host_pid = Some(true);
        assert!(!pod_matches_desired(&changed_isolation, &desired));

        let mut changed_security = api_defaulted.clone();
        changed_security
            .spec
            .as_mut()
            .unwrap()
            .security_context
            .as_mut()
            .unwrap()
            .run_as_user = Some(1_000);
        assert!(!pod_matches_desired(&changed_security, &desired));

        let mut extra_metadata = api_defaulted;
        extra_metadata
            .metadata
            .labels
            .as_mut()
            .unwrap()
            .insert("attacker".into(), "true".into());
        assert!(!pod_matches_desired(&extra_metadata, &desired));
    }

    #[test]
    fn service_adoption_accepts_api_assignment_but_rejects_exposure_drift() {
        let desired = Service {
            metadata: ObjectMeta {
                name: Some("exact-service".into()),
                namespace: Some("training".into()),
                labels: Some(BTreeMap::from([("app".into(), "spur".into())])),
                annotations: Some(BTreeMap::from([("nonce".into(), "one".into())])),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            spec: Some(ServiceSpec {
                cluster_ip: Some("None".into()),
                type_: Some("ClusterIP".into()),
                selector: Some(BTreeMap::from([("app".into(), "spur".into())])),
                ..Default::default()
            }),
            ..Default::default()
        };
        let mut api_defaulted = desired.clone();
        let spec = api_defaulted.spec.as_mut().unwrap();
        spec.cluster_ips = Some(vec!["None".into()]);
        spec.ip_families = Some(vec!["IPv4".into()]);
        spec.ip_family_policy = Some("SingleStack".into());
        assert!(service_matches_desired(&api_defaulted, &desired));

        let mut exposed = api_defaulted.clone();
        exposed.spec.as_mut().unwrap().external_ips = Some(vec!["203.0.113.1".into()]);
        assert!(!service_matches_desired(&exposed, &desired));

        let mut changed_selector = api_defaulted;
        changed_selector
            .spec
            .as_mut()
            .unwrap()
            .selector
            .as_mut()
            .unwrap()
            .insert("app".into(), "other".into());
        assert!(!service_matches_desired(&changed_selector, &desired));
    }

    #[test]
    fn control_pod_requires_owner_recorded_uid_and_dispatch_nonce() {
        let mut found = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            2,
            "worker-a",
            "submission-token-a",
        )
        .expect("launch identity");
        found.pod_dispatch_token = "dispatch-a".into();
        let requested = PodExecutionIdentity::from_request(41, "generation-a", 2, "worker-a")
            .expect("control identity");
        let pod_name = execution_pod_name(&found, "worker-a");
        let mut annotations = found.annotations();
        annotations.insert(PROVENANCE_RECORDED_ANNOTATION.into(), "true".into());
        let pod = Pod {
            metadata: ObjectMeta {
                name: Some(pod_name.clone()),
                uid: Some("pod-uid-a".into()),
                labels: Some(found.execution_labels()),
                annotations: Some(annotations),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            ..Default::default()
        };
        let status = SpurJobStatus {
            spur_job_id: Some(41),
            submission_generation: Some("generation-a".into()),
            submission_token: Some("submission-token-a".into()),
            pod_uids: BTreeMap::from([(pod_name.clone(), "pod-uid-a".into())]),
            pod_dispatch_tokens: BTreeMap::from([(pod_name, "dispatch-a".into())]),
            ..Default::default()
        };

        assert!(status_binds_control_pod(
            &status,
            "owner-uid",
            &requested,
            &pod
        ));
        assert_eq!(
            classify_control_pods(&status, "owner-uid", &requested, vec![pod.clone()], false)
                .expect("exact Pod classification")
                .len(),
            1
        );

        let mut forged_uid = pod.clone();
        forged_uid.metadata.uid = Some("forged-uid".into());
        assert!(!status_binds_control_pod(
            &status,
            "owner-uid",
            &requested,
            &forged_uid
        ));
        assert!(
            classify_control_pods(&status, "owner-uid", &requested, vec![forged_uid], false,)
                .is_err()
        );

        let mut forged_owner = pod.clone();
        forged_owner.metadata.owner_references = Some(vec![test_owner("attacker-uid")]);
        assert!(!status_binds_control_pod(
            &status,
            "owner-uid",
            &requested,
            &forged_owner
        ));

        let mut recovering = status.clone();
        recovering.pod_uids.clear();
        assert!(classify_control_pods(
            &recovering,
            "owner-uid",
            &requested,
            vec![pod.clone()],
            false,
        )
        .is_err());
        assert_eq!(
            classify_control_pods(
                &recovering,
                "owner-uid",
                &requested,
                vec![pod.clone()],
                true,
            )
            .expect("cleanup may recover a nonce-bound create response loss")
            .len(),
            1
        );

        let foreign_malformed = Pod {
            metadata: ObjectMeta {
                name: Some("foreign".into()),
                labels: Some(found.execution_labels()),
                owner_references: Some(vec![test_owner("attacker-uid")]),
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(classify_control_pods(
            &status,
            "owner-uid",
            &requested,
            vec![foreign_malformed],
            false,
        )
        .expect("foreign malformed identity must be ignored")
        .is_empty());

        let stale_attempt = PodExecutionIdentity::from_request(41, "generation-a", 1, "worker-a")
            .expect("stale control identity");
        assert!(!status_binds_control_pod(
            &status,
            "owner-uid",
            &stale_attempt,
            &pod
        ));
    }

    #[test]
    fn control_service_requires_owner_and_recorded_immutable_uid() {
        let identity = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            2,
            "worker-a",
            "submission-token-a",
        )
        .expect("service identity");
        let name = execution_service_name(&identity);
        let service = Service {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                uid: Some("service-uid-a".into()),
                labels: Some(identity.execution_labels()),
                annotations: Some(identity.service_annotations("service-dispatch-token")),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            ..Default::default()
        };
        let status = SpurJobStatus {
            spur_job_id: Some(41),
            submission_generation: Some("generation-a".into()),
            submission_token: Some("submission-token-a".into()),
            service_uids: BTreeMap::from([(name.clone(), "service-uid-a".into())]),
            service_dispatch_tokens: BTreeMap::from([(name, "service-dispatch-token".into())]),
            ..Default::default()
        };
        assert!(status_binds_control_service(
            &status,
            "owner-uid",
            &identity,
            &service
        ));
        assert_eq!(
            classify_control_services(
                &status,
                "owner-uid",
                &identity,
                vec![service.clone()],
                false,
            )
            .expect("exact Service classification")
            .len(),
            1
        );

        let mut forged = service.clone();
        forged.metadata.uid = Some("forged-service-uid".into());
        assert!(!status_binds_control_service(
            &status,
            "owner-uid",
            &identity,
            &forged
        ));
        assert!(
            classify_control_services(&status, "owner-uid", &identity, vec![forged], false,)
                .is_err()
        );

        let mut recovering = status.clone();
        recovering.service_uids.clear();
        assert!(classify_control_services(
            &recovering,
            "owner-uid",
            &identity,
            vec![service.clone()],
            false,
        )
        .is_err());
        assert_eq!(
            classify_control_services(
                &recovering,
                "owner-uid",
                &identity,
                vec![service.clone()],
                true,
            )
            .expect("cleanup may recover a nonce-bound Service create response loss")
            .len(),
            1
        );

        let stale_attempt = PodExecutionIdentity::from_launch_request(
            41,
            "generation-a",
            1,
            "worker-a",
            "submission-token-a",
        )
        .expect("stale service identity");
        assert!(!status_binds_control_service(
            &status,
            "owner-uid",
            &stale_attempt,
            &service
        ));
    }

    // --- parse_mounts: hostPath ---

    #[test]
    fn test_parse_mounts_hostpath() {
        let mounts = vec!["/data:/mnt/data:ro".to_string(), "/tmp:/tmp".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 2);
        assert_eq!(vmounts.len(), 2);
        assert_eq!(vmounts[0].mount_path, "/mnt/data");
        assert_eq!(vmounts[0].read_only, Some(true));
        assert_eq!(vmounts[1].mount_path, "/tmp");
        assert_eq!(vmounts[1].read_only, Some(false));
    }

    #[test]
    fn test_parse_mounts_hostpath_source_path() {
        let mounts = vec!["/host/data:/container/data".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 1);
        let hp = vols[0].host_path.as_ref().unwrap();
        assert_eq!(hp.path, "/host/data");
        assert_eq!(hp.type_.as_deref(), Some("DirectoryOrCreate"));
    }

    #[test]
    fn test_parse_mounts_hostpath_volume_naming() {
        let mounts = vec![
            "/a:/b".to_string(),
            "/c:/d".to_string(),
            "/e:/f".to_string(),
        ];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols[0].name, "hostpath-0");
        assert_eq!(vols[1].name, "hostpath-1");
        assert_eq!(vols[2].name, "hostpath-2");
        // Volume mount names must match volume names
        assert_eq!(vmounts[0].name, "hostpath-0");
        assert_eq!(vmounts[1].name, "hostpath-1");
        assert_eq!(vmounts[2].name, "hostpath-2");
    }

    // --- parse_mounts: PVC ---

    #[test]
    fn test_parse_mounts_pvc() {
        let mounts = vec!["pvc:my-claim:/data".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 1);
        assert_eq!(vmounts.len(), 1);
        assert_eq!(vmounts[0].mount_path, "/data");
        assert!(vols[0].persistent_volume_claim.is_some());
    }

    #[test]
    fn test_parse_mounts_pvc_claim_name() {
        let mounts = vec!["pvc:training-data:/mnt/data".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.claim_name, "training-data");
    }

    #[test]
    fn test_parse_mounts_pvc_readonly() {
        let mounts = vec!["pvc:datasets:/data:ro".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.read_only, Some(true));
        assert_eq!(vmounts[0].read_only, Some(true));
    }

    #[test]
    fn test_parse_mounts_pvc_readwrite() {
        let mounts = vec!["pvc:output:/results".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        let pvc = vols[0].persistent_volume_claim.as_ref().unwrap();
        assert_eq!(pvc.read_only, Some(false));
        assert_eq!(vmounts[0].read_only, Some(false));
    }

    #[test]
    fn test_parse_mounts_pvc_naming() {
        let mounts = vec!["pvc:a:/x".to_string(), "pvc:b:/y".to_string()];
        let (vols, _) = parse_mounts(&mounts);
        assert_eq!(vols[0].name, "pvc-0");
        assert_eq!(vols[1].name, "pvc-1");
    }

    // --- parse_mounts: mixed and edge cases ---

    #[test]
    fn test_parse_mounts_mixed_hostpath_and_pvc() {
        let mounts = vec![
            "/data:/mnt/data:ro".to_string(),
            "pvc:checkpoints:/checkpoints".to_string(),
            "/logs:/var/log".to_string(),
        ];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert_eq!(vols.len(), 3);
        assert_eq!(vmounts.len(), 3);
        // First is hostPath
        assert!(vols[0].host_path.is_some());
        assert!(vols[0].persistent_volume_claim.is_none());
        // Second is PVC
        assert!(vols[1].persistent_volume_claim.is_some());
        assert!(vols[1].host_path.is_none());
        // Third is hostPath
        assert!(vols[2].host_path.is_some());
    }

    #[test]
    fn test_parse_mounts_empty() {
        let mounts: Vec<String> = vec![];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    #[test]
    fn test_parse_mounts_single_component_ignored() {
        // A single component (no colon) should be skipped
        let mounts = vec!["just-a-path".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    #[test]
    fn test_parse_mounts_pvc_missing_mount_path_ignored() {
        // "pvc:name" without mount path should be skipped (parts.len() < 3)
        let mounts = vec!["pvc:my-claim".to_string()];
        let (vols, vmounts) = parse_mounts(&mounts);
        assert!(vols.is_empty());
        assert!(vmounts.is_empty());
    }

    // --- GPU vendor detection ---

    #[test]
    fn test_is_nvidia_gpu_positive() {
        assert!(is_nvidia_gpu("h100"));
        assert!(is_nvidia_gpu("H100"));
        assert!(is_nvidia_gpu("h200"));
        assert!(is_nvidia_gpu("a100"));
        assert!(is_nvidia_gpu("A100"));
        assert!(is_nvidia_gpu("a10g"));
        assert!(is_nvidia_gpu("a30"));
        assert!(is_nvidia_gpu("v100"));
        assert!(is_nvidia_gpu("t4"));
        assert!(is_nvidia_gpu("T4"));
        assert!(is_nvidia_gpu("l4"));
        assert!(is_nvidia_gpu("l40s"));
        assert!(is_nvidia_gpu("L40"));
        assert!(is_nvidia_gpu("b100"));
        assert!(is_nvidia_gpu("b200"));
        assert!(is_nvidia_gpu("gb200"));
        assert!(is_nvidia_gpu("GB200"));
        assert!(is_nvidia_gpu("rtx4090"));
        assert!(is_nvidia_gpu("RTX3090"));
        assert!(is_nvidia_gpu("nvidia"));
        assert!(is_nvidia_gpu("NVIDIA"));
    }

    #[test]
    fn test_is_nvidia_gpu_negative_amd() {
        assert!(!is_nvidia_gpu("mi300x"));
        assert!(!is_nvidia_gpu("MI300X"));
        assert!(!is_nvidia_gpu("mi250x"));
        assert!(!is_nvidia_gpu("mi210"));
        assert!(!is_nvidia_gpu("mi100"));
        assert!(!is_nvidia_gpu("gfx942"));
        assert!(!is_nvidia_gpu("gfx1201"));
        assert!(!is_nvidia_gpu("gfx90a"));
        assert!(!is_nvidia_gpu("rx7900xtx"));
        assert!(!is_nvidia_gpu("w7900"));
        assert!(!is_nvidia_gpu("amd"));
        assert!(!is_nvidia_gpu("gpu"));
        assert!(!is_nvidia_gpu("any"));
        assert!(!is_nvidia_gpu(""));
    }

    #[test]
    fn test_gpu_vendor_resource_key_amd() {
        assert_eq!(gpu_vendor_resource_key(Some("mi300x")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("mi250x")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gfx942")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gfx90a")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("w7900")), "amd.com/gpu");
    }

    #[test]
    fn test_gpu_vendor_resource_key_nvidia() {
        assert_eq!(gpu_vendor_resource_key(Some("h100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("a100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("v100")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("t4")), "nvidia.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("l40s")), "nvidia.com/gpu");
    }

    #[test]
    fn test_gpu_vendor_resource_key_defaults_amd() {
        // Unknown or generic GPU types default to AMD (ROCm project)
        assert_eq!(gpu_vendor_resource_key(None), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("gpu")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("any")), "amd.com/gpu");
        assert_eq!(gpu_vendor_resource_key(Some("")), "amd.com/gpu");
    }

    #[test]
    fn test_gpu_request_to_gres_amd_types() {
        assert_eq!(gpu_request_to_gres(8, Some("mi300x")), "gpu:mi300x:8");
        assert_eq!(gpu_request_to_gres(4, Some("mi250x")), "gpu:mi250x:4");
        assert_eq!(gpu_request_to_gres(1, Some("gfx942")), "gpu:gfx942:1");
    }
}
