use std::time;

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

use crate::Profile;
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
}

/// Adds "authorization: Bearer <token>" to requests.
#[doc(hidden)]
#[derive(Debug)]
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
