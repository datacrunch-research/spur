// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::pin::pin;
use std::sync::Arc;
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};

use futures_util::{StreamExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Pod, Service};
use kube::api::{Api, DeleteParams, ListParams, Patch, PatchParams, Preconditions};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::finalizer::{self, finalizer, Event as FinalizerEvent};
use kube::runtime::watcher::Config as WatcherConfig;
use kube::Client;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::{debug, error, info, warn};

use crate::crd::{
    launch_spec_sha256, resolved_submission_user, to_core_job_spec, validate_preview_launch_fields,
    PodCompletionDelivery, SpurJob, SpurJobStatus,
};
use crate::execution_identity::{
    PodExecutionIdentity, JOB_ID_LABEL, PROVENANCE_RECORDED_ANNOTATION,
    SERVICE_DISPATCH_TOKEN_ANNOTATION, SUBMISSION_GENERATION_ANNOTATION,
    SUBMISSION_GENERATION_LABEL, SUBMISSION_TOKEN_ANNOTATION,
};
use spur_proto::proto::slurm_controller_client::SlurmControllerClient;
use spur_proto::proto::{
    CancelJobBySubmissionTokenRequest, CancelJobRequest, GetJobRequest, JobInfo,
    ReportJobStatusRequest, SubmitJobRequest,
};

const FINALIZER: &str = "spur.amd.com/cleanup";
const MAX_BACKOFF_SECS: u64 = 60;
const LABEL_PATCH_BUDGET: Duration = Duration::from_secs(3);

fn is_transient_kube_error(err: &kube::Error) -> bool {
    match err {
        kube::Error::Api(status) => status.is_conflict() || matches!(status.code, 429 | 503 | 504),
        kube::Error::HyperError(_) | kube::Error::HttpError(_) | kube::Error::Service(_) => true,
        _ => false,
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    #[error("gRPC error: {0}")]
    Grpc(#[from] tonic::Status),
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),
    #[error("{0}")]
    Other(String),
    #[error("exact controller cleanup is still pending")]
    CleanupPending,
}

/// Shared state for the reconciler.
pub struct JobControllerCtx {
    pub client: Client,
    pub ctrl_client: Mutex<SlurmControllerClient<Channel>>,
}

/// Reconcile a SpurJob: delegates to kube's finalizer for atomic cleanup management.
async fn reconcile(
    job: Arc<SpurJob>,
    ctx: Arc<JobControllerCtx>,
) -> Result<Action, ReconcileError> {
    let ns = job
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| ReconcileError::Other("SpurJob has no namespace".into()))?;
    let api: Api<SpurJob> = Api::namespaced(ctx.client.clone(), &ns);

    finalizer(&api, FINALIZER, job, |event| {
        let api = api.clone();
        let ctx = ctx.clone();
        async move {
            match event {
                FinalizerEvent::Apply(job) => handle_job(job, &api, &ctx).await,
                FinalizerEvent::Cleanup(job) => handle_deletion(&job, &ctx).await,
            }
        }
    })
    .await
    .map_err(map_finalizer_err)
}

fn map_finalizer_err(e: finalizer::Error<ReconcileError>) -> ReconcileError {
    match e {
        finalizer::Error::ApplyFailed(e) => e,
        finalizer::Error::CleanupFailed(e) => e,
        finalizer::Error::AddFinalizer(e) => ReconcileError::Kube(e),
        finalizer::Error::RemoveFinalizer(e) => ReconcileError::Kube(e),
        finalizer::Error::UnnamedObject => ReconcileError::Other("unnamed SpurJob".into()),
        finalizer::Error::InvalidFinalizer => {
            ReconcileError::Other(format!("{FINALIZER} is not a valid finalizer name"))
        }
    }
}

/// Returns true if the SpurJob has not yet been submitted to spurctld.
fn should_submit(status: &SpurJobStatus) -> bool {
    status.spur_job_id.is_none()
}

fn submit_request_for_job(job: &SpurJob, submission_token: String) -> SubmitJobRequest {
    let user = resolved_submission_user(job);
    let core_spec = to_core_job_spec(&job.spec, &user);
    SubmitJobRequest {
        spec: Some(core_job_spec_to_proto(&core_spec)),
        submission_token,
    }
}

/// Submit to spurctld, apply job-id label, and patch CRD status.
/// Re-reads from the API server first to guard against stale informer cache.
/// Returns `Ok(None)` if a prior reconcile already submitted.
async fn submit_to_controller(
    api: &Api<SpurJob>,
    ctx: &JobControllerCtx,
    name: &str,
    ns: &str,
) -> Result<Option<u32>, ReconcileError> {
    // Fresh read from API server — informer cache may be stale after finalizer patch
    let fresh = api.get(name).await.map_err(ReconcileError::Kube)?;
    let fresh_status = fresh.status.clone().unwrap_or_default();
    if !should_submit(&fresh_status) {
        debug!(spurjob = %name, "already submitted by prior reconcile");
        return Ok(None);
    }

    let submission_token = fresh_status
        .submission_token
        .as_deref()
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            ReconcileError::Other("submission token must be persisted before SubmitJob".to_string())
        })?
        .to_string();
    validate_preview_launch_fields(&fresh.spec).map_err(ReconcileError::Other)?;
    let current_digest = launch_spec_sha256(&fresh.spec, &resolved_submission_user(&fresh))
        .map_err(ReconcileError::Other)?;
    if fresh_status.launch_spec_sha256.as_deref() != Some(current_digest.as_str()) {
        return Err(ReconcileError::Other(
            "SpurJob spec changed after submission intent was persisted".to_string(),
        ));
    }

    let mut ctrl = ctx.ctrl_client.lock().await;
    let submission = match ctrl
        .submit_job(submit_request_for_job(&fresh, submission_token.clone()))
        .await
    {
        Ok(resp) => resp.into_inner(),
        Err(e) => {
            error!(spurjob = %name, error = %e, "failed to submit SpurJob");
            return Err(ReconcileError::Grpc(e));
        }
    };
    drop(ctrl);
    let job_id = submission.job_id;
    let submission_generation = (!submission.submission_generation.is_empty())
        .then_some(submission.submission_generation)
        .ok_or_else(|| {
            ReconcileError::Other(format!(
                "controller omitted submission generation for job {job_id}"
            ))
        })?;

    info!(spurjob = %name, job_id, namespace = %ns, "SpurJob submitted");

    // Exact labels and status must both be durable before the virtual agent can
    // resolve this CR. A lost patch is retried with the same controller token.
    ensure_job_identity_labels(&fresh, api, name, job_id, &submission_generation).await?;

    let new_status = SpurJobStatus {
        state: "Pending".into(),
        spur_job_id: Some(job_id),
        submission_generation: Some(submission_generation),
        submission_token: Some(submission_token),
        ..fresh_status
    };
    patch_status(api, name, &new_status).await?;

    Ok(Some(job_id))
}

async fn rescan_terminal_pods(
    api: &Api<SpurJob>,
    ctx: &JobControllerCtx,
    name: &str,
) -> Result<usize, ReconcileError> {
    let owner = api.get(name).await.map_err(ReconcileError::Kube)?;
    let status = owner
        .status
        .as_ref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no durable status".into()))?;
    let Some(job_id) = status.spur_job_id else {
        return Ok(0);
    };
    if status
        .submission_generation
        .as_deref()
        .is_none_or(|generation| generation.is_empty())
        || status
            .submission_token
            .as_deref()
            .is_none_or(|token| token.is_empty())
    {
        return Ok(0);
    }
    let namespace = owner
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no namespace".into()))?;
    let owner_uid = owner
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no UID".into()))?;
    let pods: Api<Pod> = Api::namespaced(ctx.client.clone(), namespace);
    let selector = ListParams::default().labels(&format!("{JOB_ID_LABEL}={job_id}"));
    let mut queued = 0;
    for pod in pods
        .list(&selector)
        .await
        .map_err(ReconcileError::Kube)?
        .items
    {
        if !resource_owned_by(&pod.metadata, owner_uid) {
            continue;
        }
        let Ok(identity) = PodExecutionIdentity::from_pod(&pod) else {
            continue;
        };
        let Some(pod_name) = pod.metadata.name.as_deref() else {
            continue;
        };
        let Some(pod_uid) = pod.metadata.uid.as_deref() else {
            continue;
        };
        if !status_binds_pod(status, pod_name, pod_uid, &identity)
            || pod
                .metadata
                .annotations
                .as_ref()
                .and_then(|annotations| annotations.get(PROVENANCE_RECORDED_ANNOTATION))
                .is_none_or(|value| value != "true")
            || status.completion_deliveries.contains_key(pod_uid)
        {
            continue;
        }
        let Some(delivery) =
            completion_delivery_from_pod(&pod, &identity).map_err(ReconcileError::Other)?
        else {
            continue;
        };
        persist_completion_delivery(ctx.client.clone(), &owner, &pod, &identity, &delivery).await?;
        queued += 1;
    }
    Ok(queued)
}

async fn mark_completion_delivered(
    api: &Api<SpurJob>,
    name: &str,
    pod_uid: &str,
    expected: &PodCompletionDelivery,
) -> Result<(), ReconcileError> {
    for _ in 0..8 {
        let fresh = api.get(name).await.map_err(ReconcileError::Kube)?;
        let status = fresh
            .status
            .as_ref()
            .ok_or_else(|| ReconcileError::Other("owning SpurJob has no durable status".into()))?;
        let current = status.completion_deliveries.get(pod_uid).ok_or_else(|| {
            ReconcileError::Other(format!(
                "completion outbox entry for Pod UID {pod_uid} disappeared"
            ))
        })?;
        if !same_completion_payload(current, expected) {
            return Err(ReconcileError::Other(format!(
                "completion outbox entry for Pod UID {pod_uid} changed payload"
            )));
        }
        if current.delivered {
            return Ok(());
        }
        let mut delivered = current.clone();
        delivered.delivered = true;
        let resource_version =
            fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                ReconcileError::Other("owning SpurJob has no resourceVersion".into())
            })?;
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": resource_version },
            "status": { "completionDeliveries": { (pod_uid): delivered } }
        });
        match api
            .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => return Ok(()),
            Err(kube::Error::Api(error)) if error.code == 409 => continue,
            Err(error) => return Err(ReconcileError::Kube(error)),
        }
    }
    Err(ReconcileError::Other(
        "concurrent completion acknowledgement updates did not converge".into(),
    ))
}

async fn deliver_pending_completions(
    api: &Api<SpurJob>,
    ctx: &JobControllerCtx,
    name: &str,
) -> Result<usize, ReconcileError> {
    let fresh = api.get(name).await.map_err(ReconcileError::Kube)?;
    let status = fresh
        .status
        .as_ref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no durable status".into()))?;
    let pending = pending_completion_deliveries(status);
    for (pod_uid, delivery) in &pending {
        if status.spur_job_id != Some(delivery.job_id)
            || status.submission_generation.as_deref()
                != Some(delivery.submission_generation.as_str())
            || status.submission_token.as_deref() != Some(delivery.submission_token.as_str())
            || status
                .pod_dispatch_tokens
                .get(&delivery.pod_name)
                .is_none_or(|token| token != &delivery.pod_dispatch_token)
            || status
                .pod_uids
                .get(&delivery.pod_name)
                .is_none_or(|uid| uid != pod_uid)
            || delivery.pod_uid != *pod_uid
        {
            return Err(ReconcileError::Other(format!(
                "completion outbox entry for Pod UID {pod_uid} does not match its owning submission"
            )));
        }
        let request = completion_request_from_delivery(delivery)?;
        info!(
            job_id = delivery.job_id,
            pod = %delivery.pod_name,
            generation = %delivery.submission_generation,
            attempt = delivery.run_attempt,
            "delivering durable Pod completion to spurctld"
        );
        {
            let mut ctrl = ctx.ctrl_client.lock().await;
            if let Err(report_error) = ctrl.report_job_status(request).await {
                let report_code = report_error.code();
                if !matches!(
                    report_code,
                    tonic::Code::NotFound | tonic::Code::FailedPrecondition
                ) {
                    return Err(ReconcileError::Grpc(report_error));
                }
                // A terminal submission summary is not a wildcard per-node
                // receipt. Exact tokened GetJob is the separate proof that an
                // independently terminalized outbox entry became moot.
                let terminal = ctrl
                    .get_job(GetJobRequest {
                        job_id: delivery.job_id,
                        submission_token: delivery.submission_token.clone(),
                    })
                    .await
                    .map_err(ReconcileError::Grpc)?
                    .into_inner();
                if !controller_job_makes_delivery_moot(&terminal, delivery, report_code) {
                    return Err(ReconcileError::Grpc(report_error));
                }
            }
        }
        // A crash or API outage after the accepted RPC leaves `delivered`
        // false. Reconcile retries the same exact report; spurctld accepts only
        // a byte-for-byte matching durable per-node receipt.
        mark_completion_delivered(api, name, pod_uid, delivery).await?;
    }
    Ok(pending.len())
}

fn pending_completion_deliveries(status: &SpurJobStatus) -> Vec<(String, PodCompletionDelivery)> {
    status
        .completion_deliveries
        .iter()
        .filter(|(_, delivery)| !delivery.delivered)
        .map(|(uid, delivery)| (uid.clone(), delivery.clone()))
        .collect()
}

async fn fail_closed_invalid_submission(
    api: &Api<SpurJob>,
    ctx: &JobControllerCtx,
    name: &str,
    status: &SpurJobStatus,
    message: String,
) -> Result<Action, ReconcileError> {
    if status
        .submission_token
        .as_deref()
        .is_none_or(|token| token.is_empty())
    {
        let mut rejected = status.clone();
        rejected.state = "Rejected".to_string();
        rejected.message = Some(message);
        patch_status(api, name, &rejected).await?;
        return Ok(Action::await_change());
    }

    let fence = fence_controller_cleanup(status, ctx).await?;
    let mut recovered_status = status.clone();
    if let Some((job_id, generation)) = fence.exact_submission() {
        backfill_controller_identity(
            api,
            name,
            status.submission_token.as_deref(),
            *job_id,
            generation,
        )
        .await?;
        recovered_status = api
            .get(name)
            .await
            .map_err(ReconcileError::Kube)?
            .status
            .unwrap_or_default();
    }
    match fence {
        ControllerCleanupFence::Pending(_) => {
            let mut cancelling = recovered_status;
            cancelling.state = "CancellingInvalidSpec".to_string();
            cancelling.message = Some(message);
            patch_status(api, name, &cancelling).await?;
            Err(ReconcileError::CleanupPending)
        }
        ControllerCleanupFence::Complete(_) => {
            let mut failed = recovered_status;
            failed.state = "Failed".to_string();
            failed.message = Some(message);
            patch_status(api, name, &failed).await?;
            Ok(Action::await_change())
        }
    }
}

/// State-machine dispatcher: submit if no job_id, otherwise poll spurctld.
async fn handle_job(
    job: Arc<SpurJob>,
    api: &Api<SpurJob>,
    ctx: &JobControllerCtx,
) -> Result<Action, ReconcileError> {
    let name = job.metadata.name.clone().unwrap_or_default();
    let ns = job
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| ReconcileError::Other("SpurJob has no namespace".into()))?;
    let mut status = job.status.clone().unwrap_or_default();

    if let Err(message) = validate_preview_launch_fields(&job.spec) {
        return fail_closed_invalid_submission(api, ctx, &name, &status, message).await;
    }
    let launch_digest = launch_spec_sha256(&job.spec, &resolved_submission_user(&job))
        .map_err(ReconcileError::Other)?;
    if !is_terminal(&status.state) && status.submission_token.is_some() {
        match status.launch_spec_sha256.as_deref() {
            Some(persisted) if persisted == launch_digest => {}
            Some(_) => {
                return fail_closed_invalid_submission(
                    api,
                    ctx,
                    &name,
                    &status,
                    "SpurJob spec changed after submission intent was persisted; launch is blocked"
                        .to_string(),
                )
                .await;
            }
            None => {
                return fail_closed_invalid_submission(
                    api,
                    ctx,
                    &name,
                    &status,
                    "submission token predates the immutable launch-spec fingerprint".to_string(),
                )
                .await;
            }
        }
    }

    if job
        .spec
        .array_spec
        .as_deref()
        .is_some_and(|array| !array.is_empty())
    {
        return fail_closed_invalid_submission(
            api,
            ctx,
            &name,
            &status,
            "Kubernetes SpurJob arrays are unsupported until per-task identities are available"
                .to_string(),
        )
        .await;
    }

    if status.spur_job_id.is_some() {
        let queued = rescan_terminal_pods(api, ctx, &name).await?;
        let delivered = deliver_pending_completions(api, ctx, &name).await?;
        if queued > 0 || delivered > 0 {
            debug!(spurjob = %name, queued, delivered, "processed durable completion outbox");
        }
        status = api
            .get(&name)
            .await
            .map_err(ReconcileError::Kube)?
            .status
            .unwrap_or_default();
    }

    if is_terminal(&status.state) {
        return Ok(Action::await_change());
    }

    // Phase 1: Submit (no spur_job_id yet)
    if should_submit(&status) {
        if status
            .submission_token
            .as_deref()
            .is_none_or(|token| token.is_empty())
        {
            let mut token_status = status.clone();
            token_status.submission_token = Some(spur_core::job::Uuid::new_v4().to_string());
            token_status.launch_spec_sha256 = Some(launch_digest);
            token_status.state = "Submitting".to_string();
            token_status.message = None;
            patch_status(api, &name, &token_status).await?;
            return Ok(Action::requeue(Duration::from_millis(200)));
        }
        return match submit_to_controller(api, ctx, &name, &ns).await? {
            Some(_job_id) => Ok(Action::requeue(Duration::from_secs(5))),
            None => Ok(Action::requeue(Duration::from_secs(2))),
        };
    }

    // Phase 2: Poll spurctld for state changes
    let Some(job_id) = status.spur_job_id else {
        return Err(ReconcileError::Other(
            "submission state changed without a job ID".to_string(),
        ));
    };

    if let Some(generation) = status.submission_generation.as_deref() {
        ensure_job_identity_labels(&job, api, &name, job_id, generation).await?;
    }

    let mut ctrl = ctx.ctrl_client.lock().await;

    match ctrl
        .get_job(GetJobRequest {
            job_id,
            submission_token: status.submission_token.clone().unwrap_or_default(),
        })
        .await
    {
        Ok(resp) => {
            let info = resp.into_inner();
            let spur_state = proto_job_state_to_string(info.state);

            if info.submission_generation.is_empty() {
                return Err(ReconcileError::Other(format!(
                    "controller omitted submission generation while polling job {job_id}"
                )));
            }
            let reported_generation = info.submission_generation.clone();
            let generation = match status.submission_generation.as_deref() {
                Some(current) if current == reported_generation => current.to_string(),
                Some(current) => {
                    let mut stale = status.clone();
                    stale.state = "StaleIdentity".to_string();
                    stale.message = Some(format!(
                        "controller job {job_id} now has generation {reported_generation}; preserving CR generation {current}"
                    ));
                    patch_status(api, &name, &stale).await?;
                    return Ok(Action::await_change());
                }
                None => {
                    let token_matches = status
                        .submission_token
                        .as_deref()
                        .filter(|token| !token.is_empty())
                        .is_some_and(|token| token == info.submission_token);
                    if !token_matches {
                        let mut migration = status.clone();
                        migration.state = "MigrationRequired".to_string();
                        migration.message = Some(
                            "legacy numeric-only job cannot be backfilled without a matching durable submission token"
                                .to_string(),
                        );
                        patch_status(api, &name, &migration).await?;
                        return Ok(Action::await_change());
                    }
                    reported_generation.clone()
                }
            };
            if status
                .submission_token
                .as_deref()
                .filter(|token| !token.is_empty())
                .is_some_and(|token| token != info.submission_token)
            {
                let mut stale = status.clone();
                stale.state = "StaleIdentity".to_string();
                stale.message = Some(format!(
                    "controller job {job_id} has a different durable submission token"
                ));
                patch_status(api, &name, &stale).await?;
                return Ok(Action::await_change());
            }
            ensure_job_identity_labels(&job, api, &name, job_id, &generation).await?;
            if spur_state != status.state
                || status.submission_generation.as_deref() != Some(generation.as_str())
            {
                info!(spurjob = %name, job_id, state = %spur_state, "SpurJob status changed");
                let mut new_status = status.clone();
                new_status.state = spur_state.clone();
                new_status.submission_generation = Some(generation);
                if !info.nodelist.is_empty() {
                    new_status.assigned_nodes = info
                        .nodelist
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect();
                }
                patch_status(api, &name, &new_status).await?;
            }

            if is_terminal(&spur_state) {
                Ok(Action::await_change())
            } else {
                Ok(Action::requeue(Duration::from_secs(5)))
            }
        }
        Err(e) => {
            warn!(spurjob = %name, job_id, error = %e, "failed to poll job status");
            Err(ReconcileError::Grpc(e))
        }
    }
}

/// Handle SpurJob deletion: cancel Spur job, clean up Pods/Services.
/// kube::runtime::finalizer removes spur.amd.com/cleanup automatically after this returns Ok.
fn exact_submission_for_cleanup(status: &SpurJobStatus) -> Option<(u32, String)> {
    let job_id = status.spur_job_id?;
    status
        .submission_generation
        .as_deref()
        .filter(|generation| !generation.is_empty())
        .map(|generation| (job_id, generation.to_string()))
}

enum ControllerCleanupFence {
    Pending(Option<(u32, String)>),
    Complete(Option<(u32, String)>),
}

impl ControllerCleanupFence {
    fn exact_submission(&self) -> Option<&(u32, String)> {
        match self {
            Self::Pending(exact) | Self::Complete(exact) => exact.as_ref(),
        }
    }
}

async fn fence_controller_cleanup(
    status: &SpurJobStatus,
    ctx: &JobControllerCtx,
) -> Result<ControllerCleanupFence, ReconcileError> {
    if let Some(submission_token) = status
        .submission_token
        .as_deref()
        .filter(|token| !token.is_empty())
    {
        let response = ctx
            .ctrl_client
            .lock()
            .await
            .cancel_job_by_submission_token(CancelJobBySubmissionTokenRequest {
                submission_token: submission_token.to_string(),
            })
            .await
            .map_err(ReconcileError::Grpc)?
            .into_inner();
        let exact = if response.job_id == 0 {
            if !response.submission_generation.is_empty() {
                return Err(ReconcileError::Other(
                    "token cancellation returned a generation without a job ID".to_string(),
                ));
            }
            None
        } else {
            if response.submission_generation.is_empty() {
                return Err(ReconcileError::Other(format!(
                    "token cancellation omitted generation for job {}",
                    response.job_id
                )));
            }
            Some((response.job_id, response.submission_generation))
        };
        if let Some(status_job_id) = status.spur_job_id {
            if exact.as_ref().map(|identity| identity.0) != Some(status_job_id) {
                return Err(ReconcileError::Other(
                    "token cancellation identity conflicts with durable SpurJob status".to_string(),
                ));
            }
        }
        if let Some(status_generation) = status
            .submission_generation
            .as_deref()
            .filter(|generation| !generation.is_empty())
        {
            if exact.as_ref().map(|identity| identity.1.as_str()) != Some(status_generation) {
                return Err(ReconcileError::Other(
                    "token cancellation generation conflicts with durable SpurJob status"
                        .to_string(),
                ));
            }
        }
        return Ok(if response.cleanup_complete {
            ControllerCleanupFence::Complete(exact)
        } else {
            ControllerCleanupFence::Pending(exact)
        });
    }

    // Legacy exact-only CRs cannot use the token fence. Keep their provenance
    // until the exact job has disappeared (or its numeric ID is visibly a
    // replacement), which implies controller finalization cleanup completed.
    let Some((job_id, submission_generation)) = exact_submission_for_cleanup(status) else {
        return Ok(ControllerCleanupFence::Complete(None));
    };
    let mut ctrl = ctx.ctrl_client.lock().await;
    ctrl.cancel_job(CancelJobRequest {
        job_id,
        signal: 0,
        user: String::new(),
        expected_submission_generation: submission_generation.clone(),
    })
    .await
    .map_err(ReconcileError::Grpc)?;
    match ctrl
        .get_job(GetJobRequest {
            job_id,
            submission_token: String::new(),
        })
        .await
    {
        Err(error) if error.code() == tonic::Code::NotFound => Ok(
            ControllerCleanupFence::Complete(Some((job_id, submission_generation))),
        ),
        Err(error) => Err(ReconcileError::Grpc(error)),
        Ok(response) => {
            if response.into_inner().submission_generation != submission_generation {
                Ok(ControllerCleanupFence::Complete(Some((
                    job_id,
                    submission_generation,
                ))))
            } else {
                Ok(ControllerCleanupFence::Pending(Some((
                    job_id,
                    submission_generation,
                ))))
            }
        }
    }
}

async fn backfill_controller_identity(
    api: &Api<SpurJob>,
    name: &str,
    expected_token: Option<&str>,
    job_id: u32,
    submission_generation: &str,
) -> Result<(), ReconcileError> {
    for _ in 0..8 {
        let fresh = api.get(name).await.map_err(ReconcileError::Kube)?;
        let status = fresh.status.clone().unwrap_or_default();
        if expected_token.is_some_and(|token| status.submission_token.as_deref() != Some(token)) {
            return Err(ReconcileError::Other(
                "submission token changed while recovering controller identity".to_string(),
            ));
        }
        if status
            .spur_job_id
            .is_some_and(|persisted| persisted != job_id)
            || status
                .submission_generation
                .as_deref()
                .filter(|generation| !generation.is_empty())
                .is_some_and(|persisted| persisted != submission_generation)
        {
            return Err(ReconcileError::Other(
                "controller identity conflicts with durable SpurJob status".to_string(),
            ));
        }

        if status.spur_job_id != Some(job_id)
            || status.submission_generation.as_deref() != Some(submission_generation)
        {
            let resource_version = fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                ReconcileError::Other("SpurJob has no resourceVersion".to_string())
            })?;
            let patch = serde_json::json!({
                "metadata": { "resourceVersion": resource_version },
                "status": {
                    "spurJobId": job_id,
                    "submissionGeneration": submission_generation,
                }
            });
            match api
                .patch_status(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(error)) if error.code == 409 => continue,
                Err(error) => return Err(ReconcileError::Kube(error)),
            }
        }

        let latest = api.get(name).await.map_err(ReconcileError::Kube)?;
        ensure_job_identity_labels(&latest, api, name, job_id, submission_generation).await?;
        return Ok(());
    }
    Err(ReconcileError::Other(
        "concurrent controller-identity recovery did not converge".to_string(),
    ))
}

async fn handle_deletion(job: &SpurJob, ctx: &JobControllerCtx) -> Result<Action, ReconcileError> {
    let name = job.metadata.name.clone().unwrap_or_default();
    let ns = job
        .metadata
        .namespace
        .clone()
        .ok_or_else(|| ReconcileError::Other("SpurJob has no namespace".into()))?;
    let status = job.status.clone().unwrap_or_default();
    let owner_uid = job
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| ReconcileError::Other(format!("SpurJob {name} has no UID")))?;

    info!(spurjob = %name, "handling SpurJob deletion");

    // The status identity is mandatory once submission succeeded. Never fall
    // back to numeric-ID cleanup: a delayed finalizer may run after ID reuse.
    let fence = fence_controller_cleanup(&status, ctx).await?;
    if let Some((job_id, generation)) = fence.exact_submission() {
        let api: Api<SpurJob> = Api::namespaced(ctx.client.clone(), &ns);
        backfill_controller_identity(
            &api,
            &name,
            status.submission_token.as_deref(),
            *job_id,
            generation,
        )
        .await?;
    }
    let exact_submission = match fence {
        ControllerCleanupFence::Pending(_) => {
            // The controller's durable finalization reconciler still needs the
            // CR and its immutable resource provenance to authenticate exact
            // virtual-agent cleanup. Never remove either side of that proof.
            // kube_runtime removes the finalizer after *any* successful cleanup
            // callback, regardless of the returned Action. An error is the
            // deliberate keep-finalizer signal while controller cleanup owns
            // outstanding work.
            return Err(ReconcileError::CleanupPending);
        }
        ControllerCleanupFence::Complete(exact) => exact,
    };
    if exact_submission.is_none()
        && (status.spur_job_id.is_some() || status.submission_token.is_some())
    {
        warn!(spurjob = %name, "legacy CR lacks a recoverable exact identity; skipping numeric controller cancel");
    }

    delete_owned_submission_resources(
        ctx.client.clone(),
        &ns,
        owner_uid,
        &status,
        exact_submission.as_ref(),
    )
    .await?;

    Ok(Action::await_change())
}

fn service_matches_submission(
    service: &Service,
    job_id: u32,
    generation: &str,
    submission_token: Option<&str>,
) -> bool {
    let labels = service.metadata.labels.as_ref();
    let annotations = service.metadata.annotations.as_ref();
    labels
        .and_then(|values| values.get("spur.amd.com/job-id"))
        .is_some_and(|value| value == &job_id.to_string())
        && annotations
            .and_then(|values| values.get(SUBMISSION_GENERATION_ANNOTATION))
            .is_some_and(|value| value == generation)
        && submission_token
            .filter(|token| !token.is_empty())
            .is_none_or(|token| {
                annotations
                    .and_then(|values| values.get(SUBMISSION_TOKEN_ANNOTATION))
                    .is_some_and(|value| value == token)
            })
}

fn resource_owned_by(metadata: &kube::api::ObjectMeta, owner_uid: &str) -> bool {
    metadata.owner_references.as_ref().is_some_and(|owners| {
        owners
            .iter()
            .any(|owner| owner.uid == owner_uid && owner.controller == Some(true))
    })
}

fn pod_matches_submission(pod: &Pod, job_id: u32, generation: &str) -> bool {
    PodExecutionIdentity::from_pod(pod).is_ok_and(|identity| {
        identity.key.job_id == job_id && identity.key.submission_generation == generation
    })
}

fn uid_delete_params(uid: String, immediate: bool) -> DeleteParams {
    DeleteParams {
        grace_period_seconds: immediate.then_some(0),
        preconditions: Some(Preconditions {
            uid: Some(uid),
            resource_version: None,
        }),
        ..Default::default()
    }
}

fn exact_pod_deletions(
    pods: Vec<Pod>,
    owner_uid: &str,
    recorded_uids: &std::collections::BTreeMap<String, String>,
    dispatch_tokens: &std::collections::BTreeMap<String, String>,
    submission_token: Option<&str>,
    exact_submission: Option<&(u32, String)>,
) -> Result<Vec<(String, String)>, ReconcileError> {
    pods.into_iter()
        .filter(|pod| resource_owned_by(&pod.metadata, owner_uid))
        .filter(|pod| {
            let Some(name) = pod.metadata.name.as_deref() else {
                return false;
            };
            let Some(uid) = pod.metadata.uid.as_deref() else {
                return false;
            };
            if recorded_uids
                .get(name)
                .is_some_and(|recorded| recorded == uid)
            {
                return true;
            }
            let Ok(identity) = PodExecutionIdentity::from_pod(pod) else {
                return false;
            };
            recorded_uids.get(name).is_none()
                && !identity.pod_dispatch_token.is_empty()
                && dispatch_tokens.get(name) == Some(&identity.pod_dispatch_token)
                && submission_token
                    .filter(|token| !token.is_empty())
                    .is_some_and(|token| token == identity.submission_token)
        })
        .filter(|pod| {
            exact_submission
                .is_none_or(|(job_id, generation)| pod_matches_submission(pod, *job_id, generation))
        })
        .map(|pod| match (pod.metadata.name, pod.metadata.uid) {
            (Some(name), Some(uid)) => Ok((name, uid)),
            _ => Err(ReconcileError::Other(
                "exact Pod is missing name or UID".into(),
            )),
        })
        .collect()
}

fn exact_service_deletions(
    services: Vec<Service>,
    job_id: u32,
    submission_generation: &str,
    owner_uid: &str,
    recorded_uids: &std::collections::BTreeMap<String, String>,
    dispatch_tokens: &std::collections::BTreeMap<String, String>,
    submission_token: Option<&str>,
) -> Result<Vec<(String, String)>, ReconcileError> {
    services
        .into_iter()
        .filter(|service| {
            resource_owned_by(&service.metadata, owner_uid)
                && service_matches_submission(
                    service,
                    job_id,
                    submission_generation,
                    submission_token,
                )
                && service
                    .metadata
                    .name
                    .as_deref()
                    .zip(service.metadata.uid.as_deref())
                    .is_some_and(|(name, uid)| {
                        recorded_uids
                            .get(name)
                            .is_some_and(|recorded_uid| recorded_uid == uid)
                            || (recorded_uids.get(name).is_none()
                                && service
                                    .metadata
                                    .annotations
                                    .as_ref()
                                    .and_then(|annotations| {
                                        annotations.get(SERVICE_DISPATCH_TOKEN_ANNOTATION)
                                    })
                                    .filter(|token| !token.is_empty())
                                    .is_some_and(|token| dispatch_tokens.get(name) == Some(token)))
                    })
        })
        .map(
            |service| match (service.metadata.name, service.metadata.uid) {
                (Some(name), Some(uid)) => Ok((name, uid)),
                _ => Err(ReconcileError::Other(
                    "exact Service is missing name or UID".into(),
                )),
            },
        )
        .collect()
}

async fn delete_owned_submission_resources(
    client: Client,
    namespace: &str,
    owner_uid: &str,
    status: &SpurJobStatus,
    exact_submission: Option<&(u32, String)>,
) -> Result<(), ReconcileError> {
    let pods: Api<Pod> = Api::namespaced(client.clone(), namespace);
    let services: Api<Service> = Api::namespaced(client, namespace);
    let selector = exact_submission.map_or_else(
        || ListParams::default().labels("spur.amd.com/managed-by=spur-k8s-operator"),
        |(job_id, _)| ListParams::default().labels(&format!("{JOB_ID_LABEL}={job_id}")),
    );

    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let exact_pods = exact_pod_deletions(
                pods.list(&selector)
                    .await
                    .map_err(ReconcileError::Kube)?
                    .items,
                owner_uid,
                &status.pod_uids,
                &status.pod_dispatch_tokens,
                status.submission_token.as_deref(),
                exact_submission,
            )?;
            let exact_services = match exact_submission {
                Some((job_id, generation)) => exact_service_deletions(
                    services
                        .list(&selector)
                        .await
                        .map_err(ReconcileError::Kube)?
                        .items,
                    *job_id,
                    generation,
                    owner_uid,
                    &status.service_uids,
                    &status.service_dispatch_tokens,
                    status.submission_token.as_deref(),
                )?,
                None => Vec::new(),
            };

            if exact_pods.is_empty() && exact_services.is_empty() {
                return Ok::<(), ReconcileError>(());
            }

            for (name, uid) in exact_pods {
                match pods.delete(&name, &uid_delete_params(uid, true)).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(error)) if error.code == 404 || error.code == 409 => {}
                    Err(error) => return Err(ReconcileError::Kube(error)),
                }
            }
            for (name, uid) in exact_services {
                match services.delete(&name, &uid_delete_params(uid, false)).await {
                    Ok(_) => {}
                    Err(kube::Error::Api(error)) if error.code == 404 || error.code == 409 => {}
                    Err(error) => return Err(ReconcileError::Kube(error)),
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .map_err(|_| {
        ReconcileError::Other(format!(
            "timed out deleting resources owned by SpurJob UID {owner_uid}"
        ))
    })??;
    Ok(())
}

fn error_policy(_job: Arc<SpurJob>, error: &ReconcileError, _ctx: Arc<JobControllerCtx>) -> Action {
    error!(error = %error, "SpurJob reconciler error");
    // Exponential backoff capped at MAX_BACKOFF_SECS
    if matches!(error, ReconcileError::CleanupPending) {
        Action::requeue(Duration::from_secs(1))
    } else {
        Action::requeue(Duration::from_secs(MAX_BACKOFF_SECS))
    }
}

/// Start the SpurJob controller and Pod watcher.
pub async fn run(
    client: Client,
    controller_addr: String,
    operator_namespace: String,
) -> anyhow::Result<()> {
    let url = if controller_addr.starts_with("http") {
        controller_addr
    } else {
        format!("http://{}", controller_addr)
    };
    let ctrl_client = SlurmControllerClient::connect(url)
        .await?
        .max_decoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE)
        .max_encoding_message_size(spur_proto::MAX_GRPC_MESSAGE_SIZE);

    let ctx = Arc::new(JobControllerCtx {
        client: client.clone(),
        ctrl_client: Mutex::new(ctrl_client),
    });

    let spurjobs: Api<SpurJob> = Api::all(client.clone());
    let pods: Api<Pod> = Api::all(client.clone());

    info!(namespace = %operator_namespace, "starting SpurJob controller");

    // Run pod watcher for completion callbacks in background
    let pod_ctx = ctx.clone();
    tokio::spawn(async move {
        loop {
            if let Err(e) = watch_pods(pod_ctx.clone()).await {
                error!(error = %e, "pod watcher exited; restarting");
            } else {
                warn!("pod watcher ended; restarting");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });

    Controller::new(spurjobs, WatcherConfig::default())
        .owns(
            pods,
            WatcherConfig::default().labels("spur.amd.com/managed-by=spur-k8s-operator"),
        )
        .run(reconcile, error_policy, ctx)
        .for_each(|res| async move {
            match res {
                Ok(o) => debug!(resource = ?o, "reconciled"),
                Err(e) => error!(error = %e, "reconcile failed"),
            }
        })
        .await;

    Ok(())
}

/// Watch managed Pods and persist terminal reports into their owning CR's
/// status outbox. Delivery is performed by normal reconciliation, so a one-shot
/// Pod event or controller outage cannot strand a running allocation.
async fn watch_pods(ctx: Arc<JobControllerCtx>) -> anyhow::Result<()> {
    let pods: Api<Pod> = Api::all(ctx.client.clone());

    let stream = kube::runtime::watcher::watcher(
        pods,
        kube::runtime::watcher::Config::default()
            .labels("spur.amd.com/managed-by=spur-k8s-operator"),
    );
    let mut stream = pin!(stream);

    while let Some(event) = stream.try_next().await? {
        if let kube::runtime::watcher::Event::Apply(pod)
        | kube::runtime::watcher::Event::InitApply(pod)
        | kube::runtime::watcher::Event::Delete(pod) = event
        {
            let identity = match PodExecutionIdentity::from_pod(&pod) {
                Ok(identity) => identity,
                Err(error) => {
                    warn!(
                        pod = %pod.metadata.name.as_deref().unwrap_or(""),
                        %error,
                        "ignoring managed Pod without immutable execution identity"
                    );
                    continue;
                }
            };
            let job_id = identity.key.job_id;
            let owner = match verified_owner_for_pod(ctx.client.clone(), &pod, &identity).await {
                Ok(Some(owner)) => owner,
                Ok(None) => {
                    warn!(
                        job_id,
                        pod = %pod.metadata.name.as_deref().unwrap_or(""),
                        "ignoring Pod whose UID is not bound in its exact owning SpurJob"
                    );
                    continue;
                }
                Err(error) => {
                    warn!(job_id, %error, "failed to verify Pod completion provenance");
                    continue;
                }
            };

            let delivery = match completion_delivery_from_pod(&pod, &identity) {
                Ok(Some(delivery)) => delivery,
                Ok(None) => continue,
                Err(error) => {
                    warn!(job_id, %error, "cannot build exact completion delivery");
                    continue;
                }
            };
            if let Err(error) =
                persist_completion_delivery(ctx.client.clone(), &owner, &pod, &identity, &delivery)
                    .await
            {
                // The controller's Pod ownership watch and periodic reconcile
                // independently rescan terminal Pods, so this event may be
                // dropped safely. Still log the outage for observability.
                warn!(job_id, %error, "failed to persist terminal completion; reconcile will rescan");
            } else {
                info!(
                    job_id,
                    pod = %delivery.pod_name,
                    generation = %delivery.submission_generation,
                    attempt = delivery.run_attempt,
                    "queued exact Pod completion durably"
                );
            }
        }
    }

    Ok(())
}

async fn verified_owner_for_pod(
    client: Client,
    pod: &Pod,
    identity: &PodExecutionIdentity,
) -> Result<Option<SpurJob>, kube::Error> {
    if identity.submission_token.is_empty()
        || identity.pod_dispatch_token.is_empty()
        || pod
            .metadata
            .annotations
            .as_ref()
            .and_then(|annotations| annotations.get(PROVENANCE_RECORDED_ANNOTATION))
            .is_none_or(|value| value != "true")
    {
        return Ok(None);
    }
    let Some(namespace) = pod.metadata.namespace.as_deref() else {
        return Ok(None);
    };
    let Some(pod_name) = pod.metadata.name.as_deref() else {
        return Ok(None);
    };
    let Some(pod_uid) = pod.metadata.uid.as_deref() else {
        return Ok(None);
    };
    let Some(owner) = pod.metadata.owner_references.as_ref().and_then(|owners| {
        owners.iter().find(|owner| {
            owner.controller == Some(true)
                && owner.api_version == "spur.amd.com/v1alpha1"
                && owner.kind == "SpurJob"
        })
    }) else {
        return Ok(None);
    };

    let jobs: Api<SpurJob> = Api::namespaced(client, namespace);
    let job = match jobs.get(&owner.name).await {
        Ok(job) => job,
        Err(kube::Error::Api(error)) if error.code == 404 => return Ok(None),
        Err(error) => return Err(error),
    };
    if job.metadata.uid.as_deref() != Some(owner.uid.as_str()) {
        return Ok(None);
    }
    let Some(status) = job.status.as_ref() else {
        return Ok(None);
    };
    Ok(status_binds_pod(status, pod_name, pod_uid, identity).then_some(job))
}

fn status_binds_pod(
    status: &SpurJobStatus,
    pod_name: &str,
    pod_uid: &str,
    identity: &PodExecutionIdentity,
) -> bool {
    !identity.submission_token.is_empty()
        && !identity.pod_dispatch_token.is_empty()
        && status.spur_job_id == Some(identity.key.job_id)
        && status.submission_generation.as_deref()
            == Some(identity.key.submission_generation.as_str())
        && status.submission_token.as_deref() == Some(identity.submission_token.as_str())
        && status
            .pod_dispatch_tokens
            .get(pod_name)
            .is_some_and(|token| token == &identity.pod_dispatch_token)
        && status
            .pod_uids
            .get(pod_name)
            .is_some_and(|recorded_uid| recorded_uid == pod_uid)
}

const TARGET_NODE_LABEL: &str = "spur.ai/target-node";

/// SIGKILL (9) with the OOM sentinel bit set; spurctld strips it and maps to OUT_OF_MEMORY.
const OOM_KILL_SIGNAL: i32 = 9 | spur_core::job::OOM_SIGNAL_FLAG;

/// Resolve the Spur node name for a terminal Pod completion report.
fn resolve_reporting_node(pod: &Pod) -> Option<String> {
    pod.spec
        .as_ref()
        .and_then(|s| s.node_name.as_ref())
        .filter(|n| !n.is_empty())
        .cloned()
        .or_else(|| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(TARGET_NODE_LABEL))
                .filter(|n| !n.is_empty())
                .cloned()
        })
}

/// Build a terminal callback exclusively from the identity persisted on the
/// dispatched Pod. Looking up the controller's current JobInfo/NodeInfo here
/// would let a delayed old Pod masquerade as a reused job ID or re-registered
/// worker.
fn completion_report_from_pod(
    pod: &Pod,
    identity: &PodExecutionIdentity,
    state: i32,
    exit_code: i32,
    signal: i32,
    message: String,
) -> Result<ReportJobStatusRequest, String> {
    let parsed = PodExecutionIdentity::from_pod(pod)?;
    if &parsed != identity {
        return Err("Pod annotations changed during completion handling".to_string());
    }
    let reporting_node = resolve_reporting_node(pod)
        .ok_or_else(|| "spec.nodeName and spur.ai/target-node are both missing".to_string())?;
    Ok(ReportJobStatusRequest {
        job_id: parsed.key.job_id,
        state,
        exit_code,
        signal,
        message,
        drain_node: false,
        drain_reason: String::new(),
        reporting_node,
        run_attempt: parsed.key.run_attempt,
        submission_generation: parsed.key.submission_generation,
        worker_incarnation: parsed.worker_incarnation,
        submission_token: parsed.submission_token,
    })
}

fn completion_delivery_from_pod(
    pod: &Pod,
    identity: &PodExecutionIdentity,
) -> Result<Option<PodCompletionDelivery>, String> {
    let phase = pod
        .status
        .as_ref()
        .and_then(|status| status.phase.as_deref())
        .unwrap_or("");
    let pending_failure = phase == "Pending"
        && ((pod
            .status
            .as_ref()
            .and_then(|status| status.reason.as_deref())
            == Some("UnexpectedAdmissionError"))
            || pod
                .status
                .as_ref()
                .and_then(|status| status.container_statuses.as_ref())
                .and_then(|statuses| statuses.first())
                .and_then(|status| status.state.as_ref())
                .and_then(|state| state.waiting.as_ref())
                .and_then(|waiting| waiting.reason.as_deref())
                .is_some_and(|reason| reason == "ImagePullBackOff" || reason == "ErrImagePull"));

    let pod_name = pod
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| "terminal Pod has no name".to_string())?;
    let pod_uid = pod
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| "terminal Pod has no UID".to_string())?;
    let (exit_code, message, oom) = if pending_failure {
        (
            1,
            pod.status
                .as_ref()
                .and_then(|status| status.message.as_deref())
                .unwrap_or("Pod rejected by kubelet before starting")
                .to_string(),
            false,
        )
    } else {
        match phase {
            "Succeeded" => (0, format!("Pod {pod_name} Succeeded"), false),
            "Failed" => {
                let (_, exit_code, message, oom) = extract_failure_details(pod);
                (exit_code, message, oom)
            }
            _ => return Ok(None),
        }
    };

    // OOM is encoded via the signal sentinel so the wire state stays a valid
    // completion report; spurctld maps it to OUT_OF_MEMORY.
    let (state, report_exit, signal) = if oom {
        (spur_core::job::JobState::Completed, 0, OOM_KILL_SIGNAL)
    } else {
        (
            spur_core::job::JobState::completion_state_for_exit_code(exit_code),
            exit_code,
            0,
        )
    };
    let request = completion_report_from_pod(
        pod,
        identity,
        state.to_proto_i32(),
        report_exit,
        signal,
        message,
    )?;
    Ok(Some(PodCompletionDelivery {
        pod_name: pod_name.to_string(),
        pod_uid: pod_uid.to_string(),
        job_id: request.job_id,
        state: request.state,
        exit_code: request.exit_code,
        signal: request.signal,
        message: request.message,
        reporting_node: request.reporting_node,
        run_attempt: request.run_attempt,
        submission_generation: request.submission_generation,
        submission_token: identity.submission_token.clone(),
        worker_incarnation: request.worker_incarnation,
        pod_dispatch_token: identity.pod_dispatch_token.clone(),
        delivered: false,
    }))
}

fn completion_request_from_delivery(
    delivery: &PodCompletionDelivery,
) -> Result<ReportJobStatusRequest, ReconcileError> {
    if delivery.pod_uid.is_empty()
        || delivery.submission_generation.is_empty()
        || delivery.submission_token.is_empty()
        || delivery.worker_incarnation.is_empty()
        || delivery.pod_dispatch_token.is_empty()
        || delivery.reporting_node.is_empty()
        || delivery.run_attempt == 0
    {
        return Err(ReconcileError::Other(format!(
            "completion delivery for Pod {} lacks exact identity",
            delivery.pod_name
        )));
    }
    let state = spur_core::job::JobState::from_proto_i32(delivery.state).ok_or_else(|| {
        ReconcileError::Other(format!(
            "completion delivery for Pod {} has invalid state {}",
            delivery.pod_name, delivery.state
        ))
    })?;
    spur_core::job::JobState::validate_completion_report_state(state, delivery.exit_code)
        .map_err(|error| ReconcileError::Other(error.to_string()))?;
    Ok(ReportJobStatusRequest {
        job_id: delivery.job_id,
        state: delivery.state,
        exit_code: delivery.exit_code,
        signal: delivery.signal,
        message: delivery.message.clone(),
        drain_node: false,
        drain_reason: String::new(),
        reporting_node: delivery.reporting_node.clone(),
        run_attempt: delivery.run_attempt,
        submission_generation: delivery.submission_generation.clone(),
        worker_incarnation: delivery.worker_incarnation.clone(),
        submission_token: delivery.submission_token.clone(),
    })
}

fn controller_job_makes_delivery_moot(
    info: &JobInfo,
    delivery: &PodCompletionDelivery,
    report_code: tonic::Code,
) -> bool {
    let exact_submission = info.job_id == delivery.job_id
        && info.submission_generation == delivery.submission_generation
        && info.submission_token == delivery.submission_token;
    if !exact_submission {
        return false;
    }
    match report_code {
        // The exact execution is gone. Its permanent terminal summary may be
        // for this attempt or for a later attempt that superseded it.
        tonic::Code::NotFound => {
            info.run_attempt >= delivery.run_attempt
                && spur_core::job::JobState::from_proto_i32(info.state)
                    .is_some_and(|state| state.is_terminal())
        }
        // A live later attempt makes this old outbox entry irrelevant. Never
        // use GetJob to suppress a same-attempt receipt conflict.
        tonic::Code::FailedPrecondition => info.run_attempt > delivery.run_attempt,
        _ => false,
    }
}

fn same_completion_payload(left: &PodCompletionDelivery, right: &PodCompletionDelivery) -> bool {
    let mut left = left.clone();
    let mut right = right.clone();
    left.delivered = false;
    right.delivered = false;
    left == right
}

async fn persist_completion_delivery(
    client: Client,
    owner: &SpurJob,
    pod: &Pod,
    identity: &PodExecutionIdentity,
    delivery: &PodCompletionDelivery,
) -> Result<(), ReconcileError> {
    let namespace = owner
        .metadata
        .namespace
        .as_deref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no namespace".into()))?;
    let owner_name = owner
        .metadata
        .name
        .as_deref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no name".into()))?;
    let owner_uid = owner
        .metadata
        .uid
        .as_deref()
        .ok_or_else(|| ReconcileError::Other("owning SpurJob has no UID".into()))?;
    let jobs: Api<SpurJob> = Api::namespaced(client, namespace);

    for _ in 0..8 {
        let fresh = jobs.get(owner_name).await.map_err(ReconcileError::Kube)?;
        if fresh.metadata.uid.as_deref() != Some(owner_uid) {
            return Err(ReconcileError::Other(
                "owning SpurJob was replaced while queueing completion".into(),
            ));
        }
        let status = fresh
            .status
            .as_ref()
            .ok_or_else(|| ReconcileError::Other("owning SpurJob has no durable status".into()))?;
        if !resource_owned_by(&pod.metadata, owner_uid)
            || !status_binds_pod(status, &delivery.pod_name, &delivery.pod_uid, identity)
        {
            return Err(ReconcileError::Other(
                "terminal Pod is not bound by exact durable provenance".into(),
            ));
        }
        if let Some(existing) = status.completion_deliveries.get(&delivery.pod_uid) {
            return if same_completion_payload(existing, delivery) {
                Ok(())
            } else {
                Err(ReconcileError::Other(format!(
                    "Pod UID {} is already bound to a different completion payload",
                    delivery.pod_uid
                )))
            };
        }
        let resource_version =
            fresh.metadata.resource_version.as_deref().ok_or_else(|| {
                ReconcileError::Other("owning SpurJob has no resourceVersion".into())
            })?;
        let patch = serde_json::json!({
            "metadata": { "resourceVersion": resource_version },
            "status": { "completionDeliveries": { (&delivery.pod_uid): delivery } }
        });
        match jobs
            .patch_status(owner_name, &PatchParams::default(), &Patch::Merge(&patch))
            .await
        {
            Ok(_) => return Ok(()),
            Err(kube::Error::Api(error)) if error.code == 409 => continue,
            Err(error) => return Err(ReconcileError::Kube(error)),
        }
    }
    Err(ReconcileError::Other(
        "concurrent completion-outbox updates did not converge".into(),
    ))
}

/// Extract failure details from a Failed pod's container statuses.
/// Returns `(wire_state, exit_code, message, oom)`. OOMKilled keeps the wire
/// state JOB_FAILED and flags `oom` so the caller can encode it via the signal
/// sentinel; spurctld maps that to OUT_OF_MEMORY at finalization.
fn extract_failure_details(pod: &Pod) -> (i32, i32, String, bool) {
    let status = match pod.status.as_ref() {
        Some(s) => s,
        None => return (4, 1, "Pod failed (no status)".into(), false),
    };

    if let Some(container_statuses) = &status.container_statuses {
        for cs in container_statuses {
            if let Some(state) = &cs.state {
                if let Some(terminated) = &state.terminated {
                    let exit_code = terminated.exit_code;
                    let reason = terminated.reason.clone().unwrap_or_default();
                    let message = terminated.message.clone().unwrap_or_default();

                    if reason == "OOMKilled" {
                        return (
                            4,
                            exit_code,
                            "OOMKilled: container exceeded memory limit".into(),
                            true,
                        );
                    }

                    let msg = if !message.is_empty() {
                        format!("{}: {}", reason, message)
                    } else if !reason.is_empty() {
                        reason
                    } else {
                        format!("exit_code={}", exit_code)
                    };
                    return (4, exit_code, msg, false);
                }
                if let Some(waiting) = &state.waiting {
                    let reason = waiting.reason.clone().unwrap_or_default();
                    if reason == "ImagePullBackOff" || reason == "ErrImagePull" {
                        return (4, 1, format!("Image pull failed: {}", reason), false);
                    }
                }
            }
        }
    }

    (4, 1, "Pod failed".into(), false)
}

fn has_job_identity_labels(job: &SpurJob, job_id: u32, generation: &str) -> bool {
    let labels = job.metadata.labels.as_ref();
    labels.and_then(|values| values.get(JOB_ID_LABEL)) == Some(&job_id.to_string())
        && labels.and_then(|values| values.get(SUBMISSION_GENERATION_LABEL))
            == Some(&generation.to_string())
}

/// Ensure both routing labels contain the exact immutable controller identity.
async fn ensure_job_identity_labels(
    job: &SpurJob,
    api: &Api<SpurJob>,
    name: &str,
    job_id: u32,
    generation: &str,
) -> Result<(), ReconcileError> {
    if has_job_identity_labels(job, job_id, generation) {
        return Ok(());
    }
    let labels = job.metadata.labels.as_ref();
    if labels
        .and_then(|values| values.get(JOB_ID_LABEL))
        .is_some_and(|persisted| persisted != &job_id.to_string())
        || labels
            .and_then(|values| values.get(SUBMISSION_GENERATION_LABEL))
            .is_some_and(|persisted| persisted != generation)
    {
        return Err(ReconcileError::Other(
            "controller identity conflicts with durable SpurJob labels".to_string(),
        ));
    }
    let resource_version = job.metadata.resource_version.as_deref().ok_or_else(|| {
        ReconcileError::Other("SpurJob has no resourceVersion for identity-label CAS".to_string())
    })?;

    let patch = serde_json::json!({
        "metadata": {
            "resourceVersion": resource_version,
            "labels": {
                JOB_ID_LABEL: job_id.to_string(),
                SUBMISSION_GENERATION_LABEL: generation,
            }
        }
    });

    let result = tokio::time::timeout(
        LABEL_PATCH_BUDGET,
        (|| async {
            api.patch(name, &PatchParams::default(), &Patch::Merge(&patch))
                .await
                .map(|_| ())
        })
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(200))
                .with_max_delay(Duration::from_secs(1))
                .without_max_times(),
        )
        .when(is_transient_kube_error),
    )
    .await;

    match result {
        Ok(inner) => inner,
        Err(_elapsed) => Err(kube::Error::Api(Box::new(
            kube::core::Status::failure(
                "TimedOut",
                &format!(
                    "label patch timed out after {}s",
                    LABEL_PATCH_BUDGET.as_secs()
                ),
            )
            .with_code(504),
        ))),
    }
    .inspect(|_| info!(spurjob = %name, job_id, "applied job-id label"))
    .inspect_err(|e| warn!(spurjob = %name, job_id, error = %e, "failed to apply job-id label"))
    .map_err(ReconcileError::Kube)
}

async fn patch_status(
    api: &Api<SpurJob>,
    name: &str,
    status: &SpurJobStatus,
) -> Result<(), ReconcileError> {
    let patch = serde_json::json!({ "status": status });
    let pp = PatchParams::apply("spur-k8s-operator");
    api.patch_status(name, &pp, &Patch::Merge(&patch))
        .await
        .map(|_| ())
        .map_err(ReconcileError::Kube)
}

fn is_terminal(state: &str) -> bool {
    matches!(
        state,
        "Completed"
            | "Failed"
            | "Cancelled"
            | "Timeout"
            | "Deadline"
            | "OutOfMemory"
            | "NodeFail"
            | "Rejected"
            | "MigrationRequired"
            | "StaleIdentity"
    )
}

fn proto_job_state_to_string(state: i32) -> String {
    spur_core::job::JobState::from_proto_i32(state)
        .map(|s| format!("{s:?}"))
        .unwrap_or_else(|| "Unknown".into())
}

/// Convert a core JobSpec into proto JobSpec for gRPC submission.
fn core_job_spec_to_proto(spec: &spur_core::job::JobSpec) -> spur_proto::proto::JobSpec {
    spur_proto::proto::JobSpec {
        name: spec.name.clone(),
        partition: spec.partition.clone().unwrap_or_default(),
        account: spec.account.clone().unwrap_or_default(),
        user: spec.user.clone(),
        uid: spec.uid,
        gid: spec.gid,
        num_nodes: spec.num_nodes,
        num_tasks: spec.num_tasks,
        tasks_per_node: spec.tasks_per_node.unwrap_or(0),
        cpus_per_task: spec.cpus_per_task,
        memory_per_node_mb: spec.memory_per_node_mb.unwrap_or(0),
        memory_per_cpu_mb: spec.memory_per_cpu_mb.unwrap_or(0),
        gres: spec.gres.clone(),
        gpus: spec.gpus.as_ref().map(Into::into),
        gpus_per_node: spec.gpus_per_node.as_ref().map(Into::into),
        gpus_per_task: spec.gpus_per_task.as_ref().map(Into::into),
        script: spec.script.clone().unwrap_or_default(),
        argv: spec.argv.clone(),
        script_args: spec.script_args.clone(),
        work_dir: spec.work_dir.clone(),
        stdout_path: spec.stdout_path.clone().unwrap_or_default(),
        stderr_path: spec.stderr_path.clone().unwrap_or_default(),
        stdin_path: spec.stdin_path.clone().unwrap_or_default(),
        environment: spec.environment.clone(),
        time_limit: spec.time_limit.map(|d| prost_types::Duration {
            seconds: d.num_seconds(),
            nanos: 0,
        }),
        time_min: spec.time_min.map(|d| prost_types::Duration {
            seconds: d.num_seconds(),
            nanos: 0,
        }),
        qos: spec.qos.clone().unwrap_or_default(),
        // Proto `priority` is non-optional; 0 encodes "unset", not a base
        // priority of zero. The receiver decodes 0 back to `None`, which
        // `Job::new` then resolves to the default.
        priority: spec.priority.unwrap_or(0),
        reservation: spec.reservation.clone().unwrap_or_default(),
        dependency: spec.dependency.clone(),
        nodelist: spec.nodelist.clone().unwrap_or_default(),
        exclude: spec.exclude.clone().unwrap_or_default(),
        constraint: spec.constraint.clone().unwrap_or_default(),
        mpi: spec.mpi.clone().unwrap_or_default(),
        distribution: spec.distribution.clone().unwrap_or_default(),
        het_group: spec.het_group.unwrap_or(0),
        array_spec: spec.array_spec.clone().unwrap_or_default(),
        requeue: spec.requeue,
        exclusive: spec.exclusive,
        hold: spec.hold,
        comment: spec.comment.clone().unwrap_or_default(),
        wckey: spec.wckey.clone().unwrap_or_default(),
        container_image: spec.container_image.clone().unwrap_or_default(),
        container_mounts: spec.container_mounts.clone(),
        container_workdir: spec.container_workdir.clone().unwrap_or_default(),
        container_name: spec.container_name.clone().unwrap_or_default(),
        container_readonly: spec.container_readonly,
        container_mount_home: spec.container_mount_home,
        container_env: spec.container_env.clone(),
        container_entrypoint: spec.container_entrypoint.clone().unwrap_or_default(),
        container_remap_root: spec.container_remap_root,
        burst_buffer: spec.burst_buffer.clone().unwrap_or_default(),
        licenses: Vec::new(),
        mail_type: Vec::new(),
        mail_user: String::new(),
        interactive: false,
        srun_job: spec.srun_job,
        begin_time: spec.begin_time.map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }),
        deadline: spec.deadline.map(|dt| prost_types::Timestamp {
            seconds: dt.timestamp(),
            nanos: dt.timestamp_subsec_nanos() as i32,
        }),
        spread_job: spec.spread_job,
        topology: spec.topology.clone().unwrap_or_default(),
        host_network: spec.host_network,
        privileged: spec.privileged,
        host_ipc: spec.host_ipc,
        shm_size: spec.shm_size.clone().unwrap_or_default(),
        extra_resources: spec.extra_resources.clone(),
        open_mode: spec.open_mode.clone().unwrap_or_default(),
        pty: spec.pty,
        initial_winsize: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn test_owner(uid: &str) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
            api_version: "spur.amd.com/v1alpha1".into(),
            kind: "SpurJob".into(),
            name: "owner".into(),
            uid: uid.into(),
            controller: Some(true),
            block_owner_deletion: Some(true),
        }
    }

    fn deletion_test_pod(identity: &PodExecutionIdentity, name: &str, uid: &str) -> Pod {
        Pod {
            metadata: kube::api::ObjectMeta {
                name: Some(name.into()),
                uid: Some(uid.into()),
                labels: Some(identity.execution_labels()),
                annotations: Some(identity.annotations()),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn deletion_test_service(identity: &PodExecutionIdentity, name: &str, uid: &str) -> Service {
        Service {
            metadata: kube::api::ObjectMeta {
                name: Some(name.into()),
                uid: Some(uid.into()),
                labels: Some(identity.execution_labels()),
                annotations: Some(identity.service_annotations("")),
                owner_references: Some(vec![test_owner("owner-uid")]),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn exact_cleanup_uid_preconditions_then_relist_spares_replacements() {
        let old = PodExecutionIdentity::from_request(77, "generation-old", 1, "worker-old")
            .expect("old identity");
        let replacement = PodExecutionIdentity::from_request(77, "generation-new", 1, "worker-new")
            .expect("replacement identity");
        let exact = (77, "generation-old".to_string());
        let recorded = BTreeMap::from([("shared-name".to_string(), "pod-uid-old".to_string())]);
        let recorded_services =
            BTreeMap::from([("shared-name".to_string(), "service-uid-old".to_string())]);

        // First list: both exact old-generation objects must be deleted with
        // immutable UID preconditions, not name-only deletion.
        let pod_deletions = exact_pod_deletions(
            vec![deletion_test_pod(&old, "shared-name", "pod-uid-old")],
            "owner-uid",
            &recorded,
            &BTreeMap::new(),
            None,
            Some(&exact),
        )
        .unwrap();
        let service_deletions = exact_service_deletions(
            vec![deletion_test_service(
                &old,
                "shared-name",
                "service-uid-old",
            )],
            77,
            "generation-old",
            "owner-uid",
            &recorded_services,
            &BTreeMap::new(),
            None,
        )
        .unwrap();
        assert_eq!(
            pod_deletions,
            vec![("shared-name".into(), "pod-uid-old".into())]
        );
        assert_eq!(
            service_deletions,
            vec![("shared-name".into(), "service-uid-old".into())]
        );
        let pod_params = uid_delete_params(pod_deletions[0].1.clone(), true);
        let service_params = uid_delete_params(service_deletions[0].1.clone(), false);
        assert_eq!(pod_params.grace_period_seconds, Some(0));
        assert_eq!(service_params.grace_period_seconds, None);
        assert_eq!(
            pod_params.preconditions.unwrap().uid.as_deref(),
            Some("pod-uid-old")
        );
        assert_eq!(
            service_params.preconditions.unwrap().uid.as_deref(),
            Some("service-uid-old")
        );

        // Re-list after a 404/409 or accepted delete: same-name replacement
        // objects are not exact matches, so the cleanup loop can ACK without
        // deleting either replacement.
        assert!(exact_pod_deletions(
            vec![deletion_test_pod(
                &replacement,
                "shared-name",
                "pod-uid-new"
            )],
            "owner-uid",
            &recorded,
            &BTreeMap::new(),
            None,
            Some(&exact),
        )
        .unwrap()
        .is_empty());
        assert!(exact_service_deletions(
            vec![deletion_test_service(
                &replacement,
                "shared-name",
                "service-uid-new"
            )],
            77,
            "generation-old",
            "owner-uid",
            &recorded_services,
            &BTreeMap::new(),
            None,
        )
        .unwrap()
        .is_empty());
    }

    #[test]
    fn legacy_cleanup_requires_recorded_uid_and_owner() {
        let identity = PodExecutionIdentity::from_request(77, "generation-old", 1, "worker-old")
            .expect("identity");
        let recorded = BTreeMap::from([("owned".to_string(), "recorded-uid".to_string())]);
        let mut wrong_owner = deletion_test_pod(&identity, "owned", "recorded-uid");
        wrong_owner.metadata.owner_references = Some(vec![test_owner("other-owner")]);
        let deletions = exact_pod_deletions(
            vec![
                deletion_test_pod(&identity, "owned", "recorded-uid"),
                deletion_test_pod(&identity, "unrecorded", "unrecorded-uid"),
                wrong_owner,
            ],
            "owner-uid",
            &recorded,
            &BTreeMap::new(),
            None,
            None,
        )
        .unwrap();
        assert_eq!(deletions, vec![("owned".into(), "recorded-uid".into())]);

        let service = deletion_test_service(&identity, "legacy-service", "service-uid");
        assert!(exact_service_deletions(
            vec![service],
            77,
            "generation-old",
            "owner-uid",
            &BTreeMap::new(),
            &BTreeMap::new(),
            None,
        )
        .expect("filter legacy Service")
        .is_empty());
    }

    #[test]
    fn cleanup_pending_uses_error_path_that_keeps_the_finalizer() {
        let status = SpurJobStatus {
            state: "Submitting".into(),
            submission_token: Some("durable-token-a".into()),
            ..Default::default()
        };
        assert_eq!(exact_submission_for_cleanup(&status), None);
        assert!(matches!(
            ReconcileError::CleanupPending,
            ReconcileError::CleanupPending
        ));
    }

    #[tokio::test]
    async fn kube_finalizer_removes_only_after_cleanup_succeeds() {
        use std::convert::Infallible;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let mut job: SpurJob = serde_json::from_value(serde_json::json!({
            "apiVersion": "spur.amd.com/v1alpha1",
            "kind": "SpurJob",
            "metadata": {
                "name": "finalizing",
                "namespace": "training",
                "uid": "owner-uid",
                "resourceVersion": "7",
                "finalizers": [FINALIZER],
                "deletionTimestamp": "2026-08-16T00:00:00Z"
            },
            "spec": { "name": "job", "image": "image:v1" }
        }))
        .expect("deleting SpurJob");
        job.status = Some(SpurJobStatus::default());

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_service = calls.clone();
        let response_job = job.clone();
        let service = tower::service_fn(move |_request: http::Request<kube::client::Body>| {
            calls_for_service.fetch_add(1, Ordering::SeqCst);
            let body = serde_json::to_vec(&response_job).expect("serialize mock response");
            async move { Ok::<_, Infallible>(http::Response::new(kube::client::Body::from(body))) }
        });
        let api: Api<SpurJob> = Api::namespaced(Client::new(service, "training"), "training");
        let job = Arc::new(job);

        let pending = finalizer(&api, FINALIZER, job.clone(), |_| async {
            Err::<Action, _>(ReconcileError::CleanupPending)
        })
        .await;
        assert!(matches!(
            pending,
            Err(finalizer::Error::CleanupFailed(
                ReconcileError::CleanupPending
            ))
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "cleanup failure must not issue the finalizer-removal PATCH"
        );

        finalizer(&api, FINALIZER, job, |_| async {
            Ok::<_, ReconcileError>(Action::await_change())
        })
        .await
        .expect("successful cleanup removes the finalizer");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn cancel_recovery_backfills_submit_crash_identity_before_cleanup_waits() {
        use std::convert::Infallible;

        let job: SpurJob = serde_json::from_value(serde_json::json!({
            "apiVersion": "spur.amd.com/v1alpha1",
            "kind": "SpurJob",
            "metadata": {
                "name": "recovering",
                "namespace": "training",
                "uid": "owner-uid",
                "resourceVersion": "1",
                "finalizers": [FINALIZER]
            },
            "spec": { "name": "job", "image": "image:v1" },
            "status": {
                "state": "Submitting",
                "submissionToken": "durable-token"
            }
        }))
        .expect("pre-status-crash SpurJob");
        let stored = Arc::new(Mutex::new(job));
        let stored_for_service = stored.clone();
        let service = tower::service_fn(move |request: http::Request<kube::client::Body>| {
            let stored = stored_for_service.clone();
            async move {
                let method = request.method().clone();
                let uri = request.uri().to_string();
                let body = request
                    .into_body()
                    .collect_bytes()
                    .await
                    .expect("collect mock request body");
                let mut job = stored.lock().await;
                if method == http::Method::PATCH {
                    let patch: serde_json::Value =
                        serde_json::from_slice(&body).expect("merge patch JSON");
                    if uri.contains("/status") {
                        let status = job.status.get_or_insert_default();
                        status.spur_job_id = patch
                            .pointer("/status/spurJobId")
                            .and_then(serde_json::Value::as_u64)
                            .map(|value| value as u32);
                        status.submission_generation = patch
                            .pointer("/status/submissionGeneration")
                            .and_then(serde_json::Value::as_str)
                            .map(str::to_string);
                    } else if let Some(labels) = patch
                        .pointer("/metadata/labels")
                        .and_then(serde_json::Value::as_object)
                    {
                        let current = job.metadata.labels.get_or_insert_default();
                        for (key, value) in labels {
                            current.insert(
                                key.clone(),
                                value.as_str().expect("label string").to_string(),
                            );
                        }
                    }
                    let next = job
                        .metadata
                        .resource_version
                        .as_deref()
                        .and_then(|value| value.parse::<u32>().ok())
                        .unwrap_or_default()
                        + 1;
                    job.metadata.resource_version = Some(next.to_string());
                }
                let response = serde_json::to_vec(&*job).expect("serialize mock SpurJob");
                Ok::<_, Infallible>(http::Response::new(kube::client::Body::from(response)))
            }
        });
        let api: Api<SpurJob> = Api::namespaced(Client::new(service, "training"), "training");

        backfill_controller_identity(
            &api,
            "recovering",
            Some("durable-token"),
            41,
            "generation-a",
        )
        .await
        .expect("recover identity committed by Submit before cleanup remains pending");

        let recovered = stored.lock().await;
        let status = recovered.status.as_ref().unwrap();
        assert_eq!(status.spur_job_id, Some(41));
        assert_eq!(
            status.submission_generation.as_deref(),
            Some("generation-a")
        );
        assert!(has_job_identity_labels(&recovered, 41, "generation-a"));
        assert_eq!(status.submission_token.as_deref(), Some("durable-token"));
    }

    #[test]
    fn completion_requires_durable_dispatch_token_and_pod_uid_binding() {
        let mut identity = PodExecutionIdentity::from_launch_request(
            77,
            "generation-a",
            2,
            "worker-a",
            "submission-token-a",
        )
        .unwrap();
        identity.pod_dispatch_token = "pod-token-a".to_string();
        let mut status = SpurJobStatus {
            spur_job_id: Some(77),
            submission_generation: Some("generation-a".to_string()),
            submission_token: Some("submission-token-a".to_string()),
            ..Default::default()
        };

        assert!(!status_binds_pod(&status, "pod-a", "uid-a", &identity));
        status
            .pod_dispatch_tokens
            .insert("pod-a".to_string(), "pod-token-a".to_string());
        assert!(!status_binds_pod(&status, "pod-a", "uid-a", &identity));
        status
            .pod_uids
            .insert("pod-a".to_string(), "different-uid".to_string());
        assert!(!status_binds_pod(&status, "pod-a", "uid-a", &identity));
        status
            .pod_uids
            .insert("pod-a".to_string(), "uid-a".to_string());
        assert!(status_binds_pod(&status, "pod-a", "uid-a", &identity));
    }

    #[test]
    fn completion_outbox_survives_outage_and_restart_until_acknowledged() {
        let delivery = PodCompletionDelivery {
            pod_name: "pod-a".into(),
            pod_uid: "uid-a".into(),
            job_id: 77,
            state: spur_core::job::JobState::Completed.to_proto_i32(),
            exit_code: 0,
            signal: 0,
            message: "Pod pod-a Succeeded".into(),
            reporting_node: "worker-a".into(),
            run_attempt: 2,
            submission_generation: "generation-a".into(),
            submission_token: "submission-token-a".into(),
            worker_incarnation: "worker-incarnation-a".into(),
            pod_dispatch_token: "dispatch-token-a".into(),
            delivered: false,
        };
        let status = SpurJobStatus {
            state: "Running".into(),
            spur_job_id: Some(77),
            submission_generation: Some("generation-a".into()),
            submission_token: Some("submission-token-a".into()),
            pod_uids: BTreeMap::from([("pod-a".into(), "uid-a".into())]),
            pod_dispatch_tokens: BTreeMap::from([("pod-a".into(), "dispatch-token-a".into())]),
            completion_deliveries: BTreeMap::from([("uid-a".into(), delivery)]),
            ..Default::default()
        };

        // A controller outage means no acknowledgement mutation occurs. The
        // serialized CR status is the restart boundary and retains the report.
        let encoded = serde_json::to_value(&status).expect("serialize status");
        let mut restarted: SpurJobStatus =
            serde_json::from_value(encoded).expect("restore status after restart");
        let pending = pending_completion_deliveries(&restarted);
        assert_eq!(pending.len(), 1);
        let request = completion_request_from_delivery(&pending[0].1)
            .expect("restored delivery remains reportable");
        assert_eq!(request.job_id, 77);
        assert_eq!(request.run_attempt, 2);
        assert_eq!(request.submission_generation, "generation-a");

        // Only an accepted report transitions the durable entry. Keeping the
        // delivered tombstone prevents InitApply/rescan from re-enqueueing it.
        restarted
            .completion_deliveries
            .get_mut("uid-a")
            .expect("delivery exists")
            .delivered = true;
        let encoded = serde_json::to_value(&restarted).expect("serialize acknowledged status");
        let restarted_again: SpurJobStatus =
            serde_json::from_value(encoded).expect("restore acknowledged status");
        assert!(pending_completion_deliveries(&restarted_again).is_empty());
        assert!(restarted_again
            .completion_deliveries
            .get("uid-a")
            .is_some_and(|delivery| delivery.delivered));
    }

    #[test]
    fn completion_fallback_moots_only_exact_terminal_or_superseded_attempt() {
        let delivery = PodCompletionDelivery {
            job_id: 77,
            run_attempt: 2,
            submission_generation: "generation-a".into(),
            submission_token: "submission-token-a".into(),
            ..Default::default()
        };
        let exact_terminal = JobInfo {
            job_id: 77,
            state: spur_core::job::JobState::Completed.to_proto_i32(),
            run_attempt: 2,
            submission_generation: "generation-a".into(),
            submission_token: "submission-token-a".into(),
            ..Default::default()
        };
        assert!(controller_job_makes_delivery_moot(
            &exact_terminal,
            &delivery,
            tonic::Code::NotFound,
        ));
        assert!(
            !controller_job_makes_delivery_moot(
                &exact_terminal,
                &delivery,
                tonic::Code::FailedPrecondition,
            ),
            "same-attempt receipt conflicts must remain errors"
        );

        let mut later_running = exact_terminal.clone();
        later_running.state = spur_core::job::JobState::Running.to_proto_i32();
        later_running.run_attempt = 3;
        assert!(controller_job_makes_delivery_moot(
            &later_running,
            &delivery,
            tonic::Code::FailedPrecondition,
        ));
        assert!(!controller_job_makes_delivery_moot(
            &later_running,
            &delivery,
            tonic::Code::NotFound,
        ));

        let mut later_terminal = later_running.clone();
        later_terminal.state = spur_core::job::JobState::Failed.to_proto_i32();
        assert!(controller_job_makes_delivery_moot(
            &later_terminal,
            &delivery,
            tonic::Code::NotFound,
        ));

        let mut wrong_token = later_terminal;
        wrong_token.submission_token = "replacement-token".into();
        assert!(!controller_job_makes_delivery_moot(
            &wrong_token,
            &delivery,
            tonic::Code::NotFound,
        ));
    }

    #[test]
    fn legacy_status_defaults_new_provenance_and_outbox_fields() {
        let status: SpurJobStatus = serde_json::from_value(serde_json::json!({
            "state": "Running",
            "spurJobId": 77
        }))
        .expect("legacy status must deserialize");
        assert!(status.service_uids.is_empty());
        assert!(status.service_dispatch_tokens.is_empty());
        assert!(status.launch_spec_sha256.is_none());
        assert!(status.completion_deliveries.is_empty());
    }

    // --- proto_job_state_to_string ---

    #[test]
    fn test_proto_job_state_to_string_all_values() {
        for &state in &spur_core::job::JobState::ALL {
            let wire = state.to_proto_i32();
            assert_eq!(proto_job_state_to_string(wire), format!("{state:?}"));
        }
        assert_eq!(proto_job_state_to_string(-1), "Unknown");
        assert_eq!(proto_job_state_to_string(99), "Unknown");
    }

    // --- is_terminal ---

    #[test]
    fn test_is_terminal() {
        assert!(is_terminal("Completed"));
        assert!(is_terminal("Failed"));
        assert!(is_terminal("Cancelled"));
        assert!(!is_terminal("Running"));
        assert!(!is_terminal("Pending"));
    }

    #[test]
    fn test_is_terminal_timeout() {
        assert!(is_terminal("Timeout"));
    }

    #[test]
    fn test_is_terminal_nodefail() {
        assert!(is_terminal("NodeFail"));
        assert!(is_terminal("Deadline"));
        assert!(is_terminal("OutOfMemory"));
    }

    #[test]
    fn test_is_terminal_non_terminal_states() {
        assert!(!is_terminal("Completing"));
        assert!(!is_terminal("Preempted"));
        assert!(!is_terminal("Suspended"));
        assert!(!is_terminal("Unknown"));
        assert!(!is_terminal(""));
    }

    // --- resolve_reporting_node ---

    fn pod_with_node_and_label(spec_node: Option<&str>, label_node: Option<&str>) -> Pod {
        use k8s_openapi::api::core::v1::PodSpec;
        let mut labels = BTreeMap::new();
        if let Some(n) = label_node {
            labels.insert(TARGET_NODE_LABEL.to_string(), n.to_string());
        }
        Pod {
            metadata: kube::api::ObjectMeta {
                labels: if labels.is_empty() {
                    None
                } else {
                    Some(labels)
                },
                ..Default::default()
            },
            spec: spec_node.map(|node_name| PodSpec {
                node_name: Some(node_name.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn resolve_reporting_node_prefers_spec_node_name() {
        let pod = pod_with_node_and_label(Some("worker1"), Some("worker2"));
        assert_eq!(resolve_reporting_node(&pod), Some("worker1".into()));
    }

    #[test]
    fn resolve_reporting_node_falls_back_to_target_node_label() {
        let pod = pod_with_node_and_label(None, Some("worker2"));
        assert_eq!(resolve_reporting_node(&pod), Some("worker2".into()));
    }

    #[test]
    fn resolve_reporting_node_returns_none_when_both_missing() {
        let pod = pod_with_node_and_label(None, None);
        assert_eq!(resolve_reporting_node(&pod), None);
    }

    #[test]
    fn resolve_reporting_node_ignores_empty_strings() {
        let pod = pod_with_node_and_label(Some(""), Some("worker2"));
        assert_eq!(resolve_reporting_node(&pod), Some("worker2".into()));
    }

    #[test]
    fn delayed_old_completion_keeps_its_dispatched_identity_after_job_id_reuse() {
        let old = PodExecutionIdentity::from_request(
            77,
            "old-submission-generation",
            4,
            "old-worker-incarnation",
        )
        .expect("valid old identity");
        let replacement = PodExecutionIdentity::from_request(
            77,
            "replacement-generation",
            1,
            "replacement-worker-incarnation",
        )
        .expect("valid replacement identity");
        let mut labels = old.execution_labels();
        labels.insert(TARGET_NODE_LABEL.into(), "worker-old".into());
        let delayed_old_pod = Pod {
            metadata: kube::api::ObjectMeta {
                name: Some("old-pod".into()),
                labels: Some(labels),
                annotations: Some(old.annotations()),
                ..Default::default()
            },
            ..Default::default()
        };

        let report = completion_report_from_pod(
            &delayed_old_pod,
            &old,
            spur_core::job::JobState::Completed.to_proto_i32(),
            0,
            0,
            "old pod completed late".into(),
        )
        .expect("old Pod remains reportable with old identity");

        assert_eq!(report.job_id, replacement.key.job_id);
        assert_eq!(report.submission_generation, old.key.submission_generation);
        assert_eq!(report.run_attempt, old.key.run_attempt);
        assert_eq!(report.worker_incarnation, old.worker_incarnation);
        assert_ne!(
            report.submission_generation,
            replacement.key.submission_generation
        );

        let delivery = PodCompletionDelivery {
            pod_name: "old-pod".into(),
            pod_uid: "old-pod-uid".into(),
            job_id: report.job_id,
            state: report.state,
            exit_code: report.exit_code,
            signal: report.signal,
            message: report.message,
            reporting_node: report.reporting_node,
            run_attempt: report.run_attempt,
            submission_generation: report.submission_generation,
            submission_token: "submission-token-old".into(),
            worker_incarnation: report.worker_incarnation,
            pod_dispatch_token: "dispatch-token-old".into(),
            delivered: false,
        };
        assert_eq!(
            completion_request_from_delivery(&delivery)
                .expect("durable old delivery remains exact")
                .submission_generation,
            old.key.submission_generation
        );
    }

    // --- extract_failure_details ---

    #[test]
    fn test_extract_failure_details_oom() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 137,
                            reason: Some("OOMKilled".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (state, exit_code, message, oom) = extract_failure_details(&pod);
        assert_eq!(state, 4, "OOM keeps wire state JOB_FAILED");
        assert!(oom, "OOM flagged out-of-band for the signal sentinel");
        assert_eq!(exit_code, 137);
        assert!(message.contains("OOMKilled"));
    }

    #[test]
    fn test_extract_failure_details_exit_code_nonzero() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 42,
                            reason: None,
                            message: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 42);
        assert!(message.contains("exit_code=42"));
    }

    #[test]
    fn test_extract_failure_details_with_reason_and_message() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 1,
                            reason: Some("Error".into()),
                            message: Some("segfault in main".into()),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 1);
        assert_eq!(message, "Error: segfault in main");
    }

    #[test]
    fn test_extract_failure_details_reason_only() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateTerminated, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        terminated: Some(ContainerStateTerminated {
                            exit_code: 2,
                            reason: Some("DeadlineExceeded".into()),
                            message: None,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (_, _, message, _oom) = extract_failure_details(&pod);
        assert_eq!(message, "DeadlineExceeded");
    }

    #[test]
    fn test_extract_failure_details_image_pull_backoff() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ImagePullBackOff".into()),
                            message: Some("Back-off pulling image".into()),
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 1);
        assert!(message.contains("ImagePullBackOff"));
    }

    #[test]
    fn test_extract_failure_details_err_image_pull() {
        use k8s_openapi::api::core::v1::{
            ContainerState, ContainerStateWaiting, ContainerStatus, PodStatus,
        };
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![ContainerStatus {
                    name: "spur-job".into(),
                    state: Some(ContainerState {
                        waiting: Some(ContainerStateWaiting {
                            reason: Some("ErrImagePull".into()),
                            message: None,
                        }),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
        };

        let (state, _, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert!(message.contains("ErrImagePull"));
    }

    #[test]
    fn test_extract_failure_details_no_status() {
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: None,
        };
        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 1);
        assert_eq!(message, "Pod failed (no status)");
    }

    #[test]
    fn test_extract_failure_details_no_container_statuses() {
        use k8s_openapi::api::core::v1::PodStatus;
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: None,
                ..Default::default()
            }),
        };
        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 1);
        assert_eq!(message, "Pod failed");
    }

    #[test]
    fn test_extract_failure_details_empty_container_statuses() {
        use k8s_openapi::api::core::v1::PodStatus;
        let pod = Pod {
            metadata: Default::default(),
            spec: None,
            status: Some(PodStatus {
                phase: Some("Failed".into()),
                container_statuses: Some(vec![]),
                ..Default::default()
            }),
        };
        let (state, exit_code, message, _oom) = extract_failure_details(&pod);
        assert_eq!(state, 4);
        assert_eq!(exit_code, 1);
        assert_eq!(message, "Pod failed");
    }

    // --- core_job_spec_to_proto ---

    #[test]
    fn test_core_job_spec_to_proto_basic() {
        let spec = spur_core::job::JobSpec {
            name: "test-job".into(),
            user: "alice".into(),
            num_nodes: 2,
            num_tasks: 4,
            cpus_per_task: 8,
            ..Default::default()
        };
        let proto = core_job_spec_to_proto(&spec);
        assert_eq!(proto.name, "test-job");
        assert_eq!(proto.user, "alice");
        assert_eq!(proto.num_nodes, 2);
        assert_eq!(proto.num_tasks, 4);
        assert_eq!(proto.cpus_per_task, 8);
    }

    #[test]
    fn test_core_job_spec_to_proto_optional_fields() {
        let spec = spur_core::job::JobSpec {
            name: "with-opts".into(),
            partition: Some("gpu".into()),
            account: Some("research".into()),
            qos: Some("high".into()),
            priority: Some(100),
            ..Default::default()
        };
        let proto = core_job_spec_to_proto(&spec);
        assert_eq!(proto.partition, "gpu");
        assert_eq!(proto.account, "research");
        assert_eq!(proto.qos, "high");
        assert_eq!(proto.priority, 100);
    }

    #[test]
    fn test_core_job_spec_to_proto_none_fields_default() {
        let spec = spur_core::job::JobSpec::default();
        let proto = core_job_spec_to_proto(&spec);
        assert_eq!(proto.partition, "");
        assert_eq!(proto.account, "");
        assert_eq!(proto.qos, "");
        assert_eq!(proto.priority, 0);
        assert!(proto.time_limit.is_none());
    }

    #[test]
    fn test_core_job_spec_to_proto_container_fields() {
        let spec = spur_core::job::JobSpec {
            container_image: Some("pytorch:latest".into()),
            container_mounts: vec!["/data:/data:ro".into()],
            container_mount_home: true,
            container_readonly: true,
            ..Default::default()
        };
        let proto = core_job_spec_to_proto(&spec);
        assert_eq!(proto.container_image, "pytorch:latest");
        assert_eq!(proto.container_mounts, vec!["/data:/data:ro"]);
        assert!(proto.container_mount_home);
        assert!(proto.container_readonly);
    }

    #[test]
    fn test_core_job_spec_to_proto_time_limit() {
        let spec = spur_core::job::JobSpec {
            time_limit: Some(chrono::Duration::seconds(7200)),
            ..Default::default()
        };
        let proto = core_job_spec_to_proto(&spec);
        let tl = proto.time_limit.unwrap();
        assert_eq!(tl.seconds, 7200);
        assert_eq!(tl.nanos, 0);
    }

    #[test]
    fn test_core_job_spec_to_proto_gres_and_deps() {
        let spec = spur_core::job::JobSpec {
            gres: vec!["gpu:mi300x:8".into()],
            dependency: vec!["afterok:42".into()],
            array_spec: Some("0-99%10".into()),
            ..Default::default()
        };
        let proto = core_job_spec_to_proto(&spec);
        assert_eq!(proto.gres, vec!["gpu:mi300x:8"]);
        assert_eq!(proto.dependency, vec!["afterok:42"]);
        assert_eq!(proto.array_spec, "0-99%10");
    }

    // --- map_finalizer_err ---

    #[test]
    fn test_map_finalizer_err_unnamed_object() {
        let err = map_finalizer_err(finalizer::Error::UnnamedObject);
        assert!(
            matches!(&err, ReconcileError::Other(msg) if msg == "unnamed SpurJob"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_map_finalizer_err_invalid_finalizer_contains_name() {
        let err = map_finalizer_err(finalizer::Error::InvalidFinalizer);
        let ReconcileError::Other(msg) = &err else {
            panic!("expected Other, got {err:?}");
        };
        assert!(
            msg.contains(FINALIZER),
            "message should name the finalizer: {msg}"
        );
        assert!(
            msg.contains("not a valid"),
            "message should say it's invalid: {msg}"
        );
    }

    #[test]
    fn test_map_finalizer_err_apply_failed_is_passthrough() {
        let inner = ReconcileError::Other("apply failure".into());
        let err = map_finalizer_err(finalizer::Error::ApplyFailed(inner));
        assert!(matches!(&err, ReconcileError::Other(msg) if msg == "apply failure"));
    }

    #[test]
    fn test_map_finalizer_err_cleanup_failed_is_passthrough() {
        let inner = ReconcileError::Other("cleanup failure".into());
        let err = map_finalizer_err(finalizer::Error::CleanupFailed(inner));
        assert!(matches!(&err, ReconcileError::Other(msg) if msg == "cleanup failure"));
    }

    // --- has_job_id_label ---

    fn make_spurjob(labels: Option<BTreeMap<String, String>>, namespace: Option<&str>) -> SpurJob {
        SpurJob {
            metadata: kube::api::ObjectMeta {
                name: Some("test-job".into()),
                namespace: namespace.map(String::from),
                labels,
                ..Default::default()
            },
            spec: crate::crd::SpurJobSpec {
                name: "test".into(),
                image: "test:latest".into(),
                gpus: Default::default(),
                num_nodes: 1,
                tasks_per_node: 1,
                cpus_per_task: 1,
                memory_per_node: None,
                time_limit: None,
                command: vec![],
                args: vec![],
                env: Default::default(),
                partition: None,
                account: None,
                volumes: vec![],
                host_network: false,
                privileged: false,
                host_ipc: false,
                shm_size: None,
                extra_resources: std::collections::HashMap::new(),
                secret_env: std::collections::HashMap::new(),
                tolerations: vec![],
                node_selector: Default::default(),
                priority_class: None,
                service_account: None,
                array_spec: None,
                dependencies: vec![],
            },
            status: None,
        }
    }

    #[test]
    fn test_has_job_identity_labels_present() {
        let labels = BTreeMap::from([
            (JOB_ID_LABEL.into(), "42".into()),
            (SUBMISSION_GENERATION_LABEL.into(), "generation-42".into()),
        ]);
        let job = make_spurjob(Some(labels), Some("default"));
        assert!(has_job_identity_labels(&job, 42, "generation-42"));
    }

    #[test]
    fn test_has_job_id_label_absent() {
        let job = make_spurjob(Some(BTreeMap::new()), Some("default"));
        assert!(!has_job_identity_labels(&job, 42, "generation-42"));
    }

    #[test]
    fn test_has_job_id_label_none_labels() {
        let job = make_spurjob(None, Some("default"));
        assert!(!has_job_identity_labels(&job, 42, "generation-42"));
    }

    #[test]
    fn test_has_job_id_label_other_labels_only() {
        let labels = BTreeMap::from([
            ("spur.amd.com/managed-by".into(), "spur-k8s-operator".into()),
            ("app".into(), "training".into()),
        ]);
        let job = make_spurjob(Some(labels), Some("default"));
        assert!(!has_job_identity_labels(&job, 42, "generation-42"));
    }

    #[test]
    fn test_partial_or_mismatched_identity_labels_are_not_accepted() {
        let labels = BTreeMap::from([
            ("spur.amd.com/managed-by".into(), "spur-k8s-operator".into()),
            (JOB_ID_LABEL.into(), "99".into()),
            (SUBMISSION_GENERATION_LABEL.into(), "generation-old".into()),
        ]);
        let job = make_spurjob(Some(labels), Some("default"));
        assert!(!has_job_identity_labels(&job, 99, "generation-new"));
    }

    // --- namespace extraction (reconcile error path) ---

    #[test]
    fn test_namespace_missing_produces_error() {
        let job = make_spurjob(None, None);
        let result = job
            .metadata
            .namespace
            .clone()
            .ok_or_else(|| ReconcileError::Other("SpurJob has no namespace".into()));
        assert!(
            matches!(&result, Err(ReconcileError::Other(msg)) if msg == "SpurJob has no namespace")
        );
    }

    #[test]
    fn test_namespace_present_is_extracted() {
        let job = make_spurjob(None, Some("ml-team"));
        let result = job
            .metadata
            .namespace
            .clone()
            .ok_or_else(|| ReconcileError::Other("SpurJob has no namespace".into()));
        assert_eq!(result.unwrap(), "ml-team");
    }

    // --- should_submit (re-read guard) ---

    #[test]
    fn test_should_submit_when_no_job_id() {
        let status = SpurJobStatus::default();
        assert!(should_submit(&status));
    }

    #[test]
    fn test_should_not_submit_when_job_id_present() {
        let status = SpurJobStatus {
            spur_job_id: Some(42),
            ..Default::default()
        };
        assert!(!should_submit(&status));
    }

    #[test]
    fn test_should_submit_ignores_state() {
        let status = SpurJobStatus {
            state: "Running".into(),
            spur_job_id: None,
            ..Default::default()
        };
        assert!(should_submit(&status));
    }

    #[test]
    fn test_should_not_submit_regardless_of_state() {
        let status = SpurJobStatus {
            state: "Pending".into(),
            spur_job_id: Some(1),
            ..Default::default()
        };
        assert!(!should_submit(&status));
    }

    #[test]
    fn finalizer_skips_numeric_cancel_without_submission_generation() {
        for submission_generation in [None, Some(String::new())] {
            let status = SpurJobStatus {
                spur_job_id: Some(42),
                submission_generation,
                ..Default::default()
            };
            assert_eq!(exact_submission_for_cleanup(&status), None);
        }
    }

    #[test]
    fn finalizer_uses_exact_submission_identity() {
        let status = SpurJobStatus {
            spur_job_id: Some(42),
            submission_generation: Some("generation-42".into()),
            ..Default::default()
        };
        assert_eq!(
            exact_submission_for_cleanup(&status),
            Some((42, "generation-42".into()))
        );
        assert_eq!(
            exact_submission_for_cleanup(&SpurJobStatus::default()),
            None
        );
    }
}
