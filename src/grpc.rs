//! Helpers for the deprecated gRPC API.

pub mod job;

use std::time;

use futures::{Stream, StreamExt, TryStreamExt, stream};
use rsa::{RsaPublicKey, pkcs8::DecodePublicKey as _};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, ClientTlsConfig},
};

#[allow(dead_code)]
#[allow(unreachable_pub)]
#[allow(unused_qualifications)]
#[allow(missing_docs)]
#[allow(clippy::enum_variant_names)]
pub mod generated {
    tonic::include_proto!("bpln_proto.commander.service.v2");
}

use crate::{
    Profile,
    grpc::generated::{
        CancelJobRequest, GetBauplanInfoRequest, JobFailure, JobId, JobSuccess, OrganizationInfo,
        SubscribeLogsRequest, cancel_job_response::CancelStatus, job_complete_event::Outcome,
        job_failure::ErrorCode, runner_event::Event as RunnerEvent,
    },
};
use generated::v2_commander_service_client::V2CommanderServiceClient;

/// A client for the deprecated gRPC API.
pub type Client = V2CommanderServiceClient<InterceptedService<Channel, AuthInterceptor>>;

impl Client {
    /// Make a client for the deprecated gRPC API.
    pub fn new_lazy(
        profile: &Profile,
        timeout: time::Duration,
    ) -> Result<Self, tonic::transport::Error> {
        let api_endpoint = profile.api_endpoint.clone();
        let channel = Channel::builder(api_endpoint)
            .tls_config(ClientTlsConfig::new().with_native_roots())?
            .timeout(timeout)
            .user_agent(&profile.user_agent)?
            .connect_lazy();

        // We check that the key is ascii when constructing the profile.
        let auth_header: MetadataValue<Ascii> =
            format!("Bearer {}", profile.api_key).parse().unwrap();
        let inner = V2CommanderServiceClient::with_interceptor(
            channel,
            AuthInterceptor { value: auth_header },
        );

        Ok(inner)
    }

    /// Cancels a running job.
    pub async fn cancel(&mut self, job_id: &str) -> Result<(), CancelJobError> {
        let req = CancelJobRequest {
            job_id: Some(JobId {
                id: job_id.to_owned(),
                ..Default::default()
            }),
        };

        let resp = self.cancel_job(req).await?.into_inner();

        match CancelStatus::try_from(resp.status) {
            Ok(CancelStatus::Success) => Ok(()),
            Ok(CancelStatus::Failure) => Err(CancelJobError::Failed(resp.message)),
            _ => Err(CancelJobError::Unknown(resp.message)),
        }
    }

    /// Fetches the organization-wide default public key, along with the key name
    /// (usually the ARN).
    pub async fn org_default_public_key(
        &mut self,
        timeout: time::Duration,
    ) -> Result<(String, RsaPublicKey), tonic::Status> {
        let mut req = tonic::Request::new(GetBauplanInfoRequest::default());
        req.set_timeout(timeout);

        let resp = self
            .get_bauplan_info(GetBauplanInfoRequest::default())
            .await?
            .into_inner();

        let Some(OrganizationInfo {
            default_parameter_secret_public_key: Some(pkey),
            default_parameter_secret_key: Some(key_name),
            ..
        }) = resp.organization_info
        else {
            return Err(tonic::Status::not_found(
                "encryption requested, but no organization-wide public key found",
            ));
        };

        let pkey = RsaPublicKey::from_public_key_pem(&pkey).map_err(|e| {
            tonic::Status::internal(format!("invalid organization-wide public key: {e}"))
        })?;

        Ok((key_name, pkey))
    }

    /// Runs a job to completion. Produces a stream of job events from commander. If
    /// an error is encountered in the initial SubscribeLogs call, then it is the
    /// first item returned from the stream.
    pub fn monitor_job(
        &mut self,
        job_id: String,
        timeout: time::Duration,
    ) -> impl Stream<Item = Result<RunnerEvent, tonic::Status>> {
        let mut req = tonic::Request::new(SubscribeLogsRequest {
            job_id: job_id.clone(),
        });
        req.set_timeout(timeout);

        let mut client = self.clone();
        stream::once(async move {
            let stream = client.subscribe_logs(req).await?.into_inner();
            Ok::<_, tonic::Status>(stream)
        })
        .try_flatten()
        .filter_map(async |ev| match ev {
            // Unwrap the nested struct.
            Ok(evt) => Some(Ok(evt.runner_event?.event?)),
            Err(e) => Some(Err(e)),
        })
    }
}

/// Adds "authorization: Bearer <token>" to requests.
#[doc(hidden)]
#[derive(Debug, Clone)]
pub struct AuthInterceptor {
    value: MetadataValue<Ascii>,
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.value.clone());
        Ok(request)
    }
}

/// An error returned when cancelling a job.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum CancelJobError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::Status),
    #[error("failed to cancel job: {0}")]
    Failed(String),
    #[error("unexpected cancel status: {0}")]
    Unknown(String),
}

/// An error reported for a job.
#[derive(Debug, Clone, thiserror::Error)]
#[allow(missing_docs)]
pub enum JobError {
    #[error("job failed: {1} ({0:?})")]
    Failed(ErrorCode, String),
    #[error("job cancelled")]
    Cancelled,
    #[error("job rejected: {0}")]
    Rejected(String),
    #[error("job hit server timeout")]
    Timeout,
    #[error("internal server error")]
    Internal,
    #[error("empty outcome")]
    Unknown,
}

/// The outcome of a job, as returned by [`Client::subscribe_logs`].
pub type JobResult = Result<JobSuccess, JobError>;

/// Parse a job outcome event as a possible [`JobError`].
pub fn interpret_outcome(outcome: Option<Outcome>) -> JobResult {
    match outcome {
        Some(outcome) => match outcome {
            Outcome::Success(job_success) => Ok(job_success),
            Outcome::Failure(JobFailure {
                error_code,
                error_message,
                ..
            }) => Err(JobError::Failed(
                error_code.try_into().unwrap_or_default(),
                error_message,
            )),
            Outcome::Cancellation(_) => Err(JobError::Cancelled),
            Outcome::Timeout(_) => Err(JobError::Timeout),
            Outcome::Rejected(job_rejected) => Err(JobError::Rejected(job_rejected.reason)),
            Outcome::HeartbeatFailure(_) => Err(JobError::Internal),
        },
        None => Err(JobError::Unknown),
    }
}
