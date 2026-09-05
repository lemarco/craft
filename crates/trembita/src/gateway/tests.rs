use super::config::validate_gateway_config;
use super::identity::{GatewayIdentity, GatewayRequest, IdentityError};
use super::opts::GatewayOpts;

struct TestIdentity;

impl GatewayIdentity for TestIdentity {
    type Identity = String;

    #[allow(clippy::unused_async_trait_impl)]
    async fn extract(&self, _: &GatewayRequest<'_>) -> Result<String, IdentityError> {
        Ok("test".into())
    }
}

#[test]
fn validate_accepts_surfaces_with_identity() {
    let config = GatewayOpts::new("127.0.0.1:1".parse().expect("addr"))
        .identity(TestIdentity)
        .surfaces(|_state| trembita_http::Gateway::new(false))
        .build_config();
    assert!(validate_gateway_config(&config).is_ok());
}

#[test]
fn validate_accepts_empty_gateway() {
    let config = GatewayOpts::new("127.0.0.1:1".parse().expect("addr")).build_config();
    assert!(validate_gateway_config(&config).is_ok());
}
