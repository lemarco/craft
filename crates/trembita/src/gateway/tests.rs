use super::config::{GatewayConfigError, validate_gateway_config};
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
fn validate_rejects_product_apis_without_identity() {
    let config = GatewayOpts::new("127.0.0.1:1".parse().expect("addr"))
        .with_jobs_api(true)
        .build_config();
    assert!(matches!(
        validate_gateway_config(&config),
        Err(GatewayConfigError::ProductApisWithoutIdentity)
    ));
}

#[test]
fn validate_rejects_protect_apis_without_identity() {
    let config = GatewayOpts::new("127.0.0.1:1".parse().expect("addr"))
        .protect_product_apis(true)
        .build_config();
    assert!(matches!(
        validate_gateway_config(&config),
        Err(GatewayConfigError::ProtectApisWithoutIdentity)
    ));
}

#[test]
fn validate_accepts_product_apis_with_identity() {
    let config = GatewayOpts::new("127.0.0.1:1".parse().expect("addr"))
        .with_jobs_api(true)
        .identity(TestIdentity)
        .build_config();
    assert!(validate_gateway_config(&config).is_ok());
}
