// Copyright (c) 2026 Advanced Micro Devices, Inc. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::{Pod, Service};
use tonic::Status;

pub(crate) const JOB_ID_LABEL: &str = "spur.amd.com/job-id";
pub(crate) const SUBMISSION_GENERATION_LABEL: &str = "spur.amd.com/submission-generation";
pub(crate) const RUN_ATTEMPT_LABEL: &str = "spur.amd.com/run-attempt";

pub(crate) const SUBMISSION_GENERATION_ANNOTATION: &str = "spur.amd.com/submission-generation";
pub(crate) const RUN_ATTEMPT_ANNOTATION: &str = "spur.amd.com/run-attempt";
pub(crate) const WORKER_INCARNATION_ANNOTATION: &str = "spur.amd.com/worker-incarnation";
pub(crate) const SUBMISSION_TOKEN_ANNOTATION: &str = "spur.amd.com/submission-token";
pub(crate) const PROVENANCE_RECORDED_ANNOTATION: &str = "spur.amd.com/provenance-recorded";
pub(crate) const POD_DISPATCH_TOKEN_ANNOTATION: &str = "spur.amd.com/pod-dispatch-token";
pub(crate) const SERVICE_DISPATCH_TOKEN_ANNOTATION: &str = "spur.amd.com/service-dispatch-token";

/// Controller-assigned identity for one submitted job execution. This is the
/// job-wide part of the identity, so it is also suitable for completion
/// aggregation and ownership of a multi-node headless Service.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct JobExecutionKey {
    pub(crate) job_id: u32,
    pub(crate) submission_generation: String,
    pub(crate) run_attempt: u32,
}

/// Exact identity of one execution on one registered virtual worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PodExecutionIdentity {
    pub(crate) key: JobExecutionKey,
    pub(crate) worker_incarnation: String,
    pub(crate) submission_token: String,
    pub(crate) pod_dispatch_token: String,
}

impl PodExecutionIdentity {
    pub(crate) fn from_request(
        job_id: u32,
        submission_generation: &str,
        run_attempt: u32,
        worker_incarnation: &str,
    ) -> Result<Self, Status> {
        if submission_generation.is_empty() {
            return Err(Status::invalid_argument(
                "submission_generation must be nonempty",
            ));
        }
        if run_attempt == 0 {
            return Err(Status::invalid_argument(
                "Kubernetes batch execution requires a nonzero run_attempt",
            ));
        }
        if worker_incarnation.is_empty() {
            return Err(Status::invalid_argument(
                "worker_incarnation must be nonempty",
            ));
        }
        Ok(Self {
            key: JobExecutionKey {
                job_id,
                submission_generation: submission_generation.to_string(),
                run_attempt,
            },
            worker_incarnation: worker_incarnation.to_string(),
            submission_token: String::new(),
            pod_dispatch_token: String::new(),
        })
    }

    pub(crate) fn from_launch_request(
        job_id: u32,
        submission_generation: &str,
        run_attempt: u32,
        worker_incarnation: &str,
        submission_token: &str,
    ) -> Result<Self, Status> {
        if submission_token.is_empty() {
            return Err(Status::invalid_argument(
                "Kubernetes LaunchJob requires a nonempty submission_token",
            ));
        }
        let mut identity = Self::from_request(
            job_id,
            submission_generation,
            run_attempt,
            worker_incarnation,
        )?;
        identity.submission_token = submission_token.to_string();
        Ok(identity)
    }

    /// Read only the immutable annotations written by the dispatching virtual
    /// agent. Never infer these fields from the controller's current job/node
    /// records: those records may already describe a numeric-ID replacement.
    pub(crate) fn from_pod(pod: &Pod) -> Result<Self, String> {
        let labels = pod
            .metadata
            .labels
            .as_ref()
            .ok_or_else(|| "Pod has no labels".to_string())?;
        let job_id = labels
            .get(JOB_ID_LABEL)
            .ok_or_else(|| format!("Pod is missing {JOB_ID_LABEL}"))?
            .parse::<u32>()
            .map_err(|error| format!("Pod has invalid {JOB_ID_LABEL}: {error}"))?;
        let annotations = pod
            .metadata
            .annotations
            .as_ref()
            .ok_or_else(|| "Pod has no execution annotations".to_string())?;
        let submission_generation = annotations
            .get(SUBMISSION_GENERATION_ANNOTATION)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| format!("Pod is missing {SUBMISSION_GENERATION_ANNOTATION}"))?;
        let run_attempt = annotations
            .get(RUN_ATTEMPT_ANNOTATION)
            .ok_or_else(|| format!("Pod is missing {RUN_ATTEMPT_ANNOTATION}"))?
            .parse::<u32>()
            .map_err(|error| format!("Pod has invalid {RUN_ATTEMPT_ANNOTATION}: {error}"))?;
        if run_attempt == 0 {
            return Err("Pod has zero run_attempt".to_string());
        }
        let worker_incarnation = annotations
            .get(WORKER_INCARNATION_ANNOTATION)
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| format!("Pod is missing {WORKER_INCARNATION_ANNOTATION}"))?;
        let submission_token = annotations
            .get(SUBMISSION_TOKEN_ANNOTATION)
            .cloned()
            .unwrap_or_default();
        let pod_dispatch_token = annotations
            .get(POD_DISPATCH_TOKEN_ANNOTATION)
            .cloned()
            .unwrap_or_default();

        Ok(Self {
            key: JobExecutionKey {
                job_id,
                submission_generation,
                run_attempt,
            },
            worker_incarnation,
            submission_token,
            pod_dispatch_token,
        })
    }

    pub(crate) fn annotations(&self) -> BTreeMap<String, String> {
        let mut annotations = BTreeMap::from([
            (
                SUBMISSION_GENERATION_ANNOTATION.to_string(),
                self.key.submission_generation.clone(),
            ),
            (
                RUN_ATTEMPT_ANNOTATION.to_string(),
                self.key.run_attempt.to_string(),
            ),
            (
                WORKER_INCARNATION_ANNOTATION.to_string(),
                self.worker_incarnation.clone(),
            ),
        ]);
        if !self.submission_token.is_empty() {
            annotations.insert(
                SUBMISSION_TOKEN_ANNOTATION.to_string(),
                self.submission_token.clone(),
            );
        }
        if !self.pod_dispatch_token.is_empty() {
            annotations.insert(
                POD_DISPATCH_TOKEN_ANNOTATION.to_string(),
                self.pod_dispatch_token.clone(),
            );
        }
        annotations
    }

    /// Labels used by a Service selector. Controller-generated generations are
    /// UUIDs and therefore valid Kubernetes label values; annotations remain
    /// the authoritative identity copied into completion reports.
    pub(crate) fn execution_labels(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (JOB_ID_LABEL.to_string(), self.key.job_id.to_string()),
            (
                SUBMISSION_GENERATION_LABEL.to_string(),
                self.key.submission_generation.clone(),
            ),
            (
                RUN_ATTEMPT_LABEL.to_string(),
                self.key.run_attempt.to_string(),
            ),
        ])
    }

    pub(crate) fn matches_pod(&self, pod: &Pod) -> bool {
        Self::from_pod(pod).is_ok_and(|found| {
            found.key == self.key
                && found.worker_incarnation == self.worker_incarnation
                && (self.submission_token.is_empty()
                    || found.submission_token == self.submission_token)
                && (self.pod_dispatch_token.is_empty()
                    || found.pod_dispatch_token == self.pod_dispatch_token)
        })
    }

    pub(crate) fn service_annotations(
        &self,
        service_dispatch_token: &str,
    ) -> BTreeMap<String, String> {
        let mut annotations = BTreeMap::from([
            (
                SUBMISSION_GENERATION_ANNOTATION.to_string(),
                self.key.submission_generation.clone(),
            ),
            (
                RUN_ATTEMPT_ANNOTATION.to_string(),
                self.key.run_attempt.to_string(),
            ),
        ]);
        if !self.submission_token.is_empty() {
            annotations.insert(
                SUBMISSION_TOKEN_ANNOTATION.to_string(),
                self.submission_token.clone(),
            );
        }
        if !service_dispatch_token.is_empty() {
            annotations.insert(
                SERVICE_DISPATCH_TOKEN_ANNOTATION.to_string(),
                service_dispatch_token.to_string(),
            );
        }
        annotations
    }

    pub(crate) fn owns_service(&self, service: &Service) -> bool {
        let Some(annotations) = service.metadata.annotations.as_ref() else {
            return false;
        };
        let labels = service.metadata.labels.as_ref();
        labels.and_then(|values| values.get(JOB_ID_LABEL)) == Some(&self.key.job_id.to_string())
            && labels.and_then(|values| values.get(SUBMISSION_GENERATION_LABEL))
                == Some(&self.key.submission_generation)
            && labels.and_then(|values| values.get(RUN_ATTEMPT_LABEL))
                == Some(&self.key.run_attempt.to_string())
            && !self.submission_token.is_empty()
            && annotations.get(SUBMISSION_TOKEN_ANNOTATION) == Some(&self.submission_token)
            && annotations.get(SUBMISSION_GENERATION_ANNOTATION)
                == Some(&self.key.submission_generation)
            && annotations.get(RUN_ATTEMPT_ANNOTATION) == Some(&self.key.run_attempt.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kube::api::ObjectMeta;

    fn pod(identity: &PodExecutionIdentity) -> Pod {
        let mut labels = identity.execution_labels();
        labels.insert("spur.amd.com/managed-by".into(), "spur-k8s-operator".into());
        Pod {
            metadata: ObjectMeta {
                labels: Some(labels),
                annotations: Some(identity.annotations()),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn pod_round_trip_preserves_immutable_execution_identity() {
        let mut identity = PodExecutionIdentity::from_launch_request(
            7,
            "generation-a",
            3,
            "worker-a",
            "submission-token-a",
        )
        .expect("valid identity");
        identity.pod_dispatch_token = "pod-token-a".to_string();
        assert_eq!(
            PodExecutionIdentity::from_pod(&pod(&identity)),
            Ok(identity)
        );
    }

    #[test]
    fn reused_numeric_job_id_does_not_match_old_pod() {
        let old = PodExecutionIdentity::from_request(7, "generation-old", 2, "worker-old")
            .expect("valid old identity");
        let replacement = PodExecutionIdentity::from_request(7, "generation-new", 1, "worker-new")
            .expect("valid replacement identity");
        let delayed_old_pod = pod(&old);

        assert!(old.matches_pod(&delayed_old_pod));
        assert!(!replacement.matches_pod(&delayed_old_pod));
        assert_eq!(
            PodExecutionIdentity::from_pod(&delayed_old_pod)
                .expect("old annotations remain authoritative"),
            old
        );
    }
}
