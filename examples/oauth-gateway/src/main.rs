//! Dev OIDC authorize + callback → cluster session cookie (B-40/B-47).

use std::sync::Arc;
use std::time::Duration;

use http::StatusCode;
use trembita::{
    AppManifest, ClusterSessionSecret, CookieConfig, Gateway, GatewayOpts, RequestCtx, Response,
    RouteTable, SignedCookieSessionIssuer, SignedCookieSessionVerifier, TrembitaApp,
    TrembitaConfigure, gateway_auth::{
        DevOidcAuthorize, DevOidcCallback, OidcProductionConfig, RedirectAllowlist,
    },
    rotating_cluster_session_gate, session_user_from_cookie,
};
use trembita_tools::showcase_common::{data_dir, http_bind_from_env};

const DATA_DIR: &str = "trembita-showcase-oauth-gateway";
const SESSION_TTL: Duration = Duration::from_secs(3600);

fn oauth_production_config(bind: &str) -> OidcProductionConfig {
    OidcProductionConfig::from_env().unwrap_or_else(|_| {
        let redirect = format!("http://{bind}/oauth/callback");
        OidcProductionConfig {
            redirect_allowlist: RedirectAllowlist::parse(&redirect),
            pkce_method: Default::default(),
        }
    })
}

fn gateway_surfaces(
    secret: ClusterSessionSecret,
    verifier: SignedCookieSessionVerifier,
    bind: String,
) -> impl Fn(trembita::TrembitaGatewayState) -> Gateway {
    let cookie = CookieConfig::from_env("OAUTH", "sess");
    let gate = rotating_cluster_session_gate(verifier, cookie.clone());
    let issuer: Arc<dyn trembita::SessionIssuer> =
        Arc::new(SignedCookieSessionIssuer::new(secret.clone()));
    let oauth_config = oauth_production_config(&bind);
    let authorize = DevOidcAuthorize::new(oauth_config.clone());
    let oidc = DevOidcCallback::new(Arc::clone(&issuer), gate.clone(), SESSION_TTL)
        .with_production(oauth_config);

    move |state| {
        let secret = secret.clone();
        let routes = RouteTable::new()
            .get("/oauth/start", {
                let authorize = authorize.clone();
                move |ctx: RequestCtx| {
                    let authorize = authorize.clone();
                    async move { authorize.handle(ctx).await }
                }
            })
            .get("/oauth/callback", {
                let oidc = oidc.clone();
                move |ctx: RequestCtx| {
                    let oidc = oidc.clone();
                    async move { oidc.handle(ctx).await }
                }
            })
            .get_session("/me", move |ctx: RequestCtx| {
                let secret = secret.clone();
                async move {
                    let user = session_user_from_cookie("sess", ctx.headers(), &secret)?;
                    Ok(Response::json(
                        StatusCode::OK,
                        serde_json::json!({ "user": user }),
                    ))
                }
            })
            .merge(state.app.ops_api().route_table());
        Gateway::new(false)
            .dev_fallback_session(gate.clone())
            .dev_fallback(routes)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    trembita::init_tracing();
    let dir = data_dir(DATA_DIR);
    let _ = std::fs::create_dir_all(&dir);
    let bind = http_bind_from_env("127.0.0.1:8395");
    let secret = ClusterSessionSecret::from_env().unwrap_or_else(|_| {
        eprintln!("warning: using showcase dev session secret (set TREMBITA_GATEWAY_SESSION_SECRET for multi-node)");
        ClusterSessionSecret::showcase_dev()
    });
    let verifier = SignedCookieSessionVerifier::from_env().unwrap_or_else(|_| {
        SignedCookieSessionVerifier::new(secret.clone())
    });

    TrembitaApp::builder()
        .manifest(AppManifest::new())
        .configure(TrembitaConfigure::default().with_data_dir(dir).with_local_gateway_apis())
        .gateway(
            GatewayOpts::new(bind).surfaces(gateway_surfaces(secret, verifier, bind.to_string())),
        )
        .run()
        .await?;
    Ok(())
}
