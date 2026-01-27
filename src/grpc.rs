use std::time;

use futures::{Stream, StreamExt, TryStreamExt, stream};
use tonic::{
    metadata::{Ascii, MetadataValue},
    service::{Interceptor, interceptor::InterceptedService},
    transport::{Channel, ClientTlsConfig},
};

#[allow(dead_code)]
#[allow(unreachable_pub)]
#[allow(unused_qualifications)]
#[allow(clippy::enum_variant_names)]
pub mod generated {
    tonic::include_proto!("bpln_proto.commander.service.v2");
}

use crate::{
    Profile,
    grpc::generated::{
        JobFailure, JobSuccess, SubscribeLogsRequest, job_complete_event::Outcome,
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

/// An error reported for a job.
#[derive(Debug, Clone, thiserror::Error)]
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
