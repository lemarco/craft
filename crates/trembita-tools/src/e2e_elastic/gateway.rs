//! Cluster session + `/e2e/whoami` cap smoke for LB E2E.

/// HTTP path for PerNode cap smoke ([`e2e/elastic_lb.sh`](../../../../e2e/elastic_lb.sh), B-42 local cluster).
pub const E2E_WHOAMI_PATH: &str = "/e2e/whoami";

use std::time::Duration;

use http::StatusCode;
use trembita::{
    CapVia, ClusterSessionSecret, CookieConfig, HttpError, RequestCtx, Response, RouteTable,
    SessionGate, TrembitaGatewayState, cluster_session_gate, session_user_from_cookie,
};

use super::cap::{NodeReply, WhoAmI};

const SESSION_TTL: Duration = Duration::from_secs(3600);

fn session_wiring() -> (ClusterSessionSecret, SessionGate, String) {
    let secret = ClusterSessionSecret::from_env().unwrap_or_else(|_| {
        eprintln!(
            "warning: TREMBITA_GATEWAY_SESSION_SECRET unset — using e2e dev fallback (do not use in prod)"
        );
        ClusterSessionSecret::from_bytes(b"e2e-elastic-secret-16b").expect("dev secret len")
    });
    let cookie = CookieConfig::from_env("E2E_ELASTIC", "sess");
    let name = cookie.name.clone();
    let gate = cluster_session_gate(secret.clone(), cookie);
    (secret, gate, name)
}

/// Inline **PerNode** cap smoke route (local cluster + elastic E2E).
#[must_use]
pub fn mount_whoami(table: RouteTable, state: TrembitaGatewayState) -> RouteTable {
    table.get(E2E_WHOAMI_PATH, move |_ctx: RequestCtx| {
        let st = state.clone();
        async move {
            let reply: NodeReply = WhoAmI
                .via(st.app.as_ref())
                .route(trembita::Route::Inline)
                .await
                .map_err(|e| HttpError::Internal(e.to_string()))?;
            Ok(Response::json(
                StatusCode::OK,
                serde_json::to_value(reply).unwrap_or_default(),
            ))
        }
    })
}

#[must_use]
pub fn route_table(state: TrembitaGatewayState) -> RouteTable {
    let (secret, gate, cookie_name) = session_wiring();
    let login_secret = secret.clone();
    let login_gate = gate.clone();
    let me_secret = secret.clone();
    let whoami_state = state.clone();

    mount_whoami(
        RouteTable::new()
            .get("/login", move |ctx: RequestCtx| {
                let gate = login_gate.clone();
                let secret = login_secret.clone();
                async move {
                    let user = ctx.query_param("user").unwrap_or("anonymous").to_string();
                    let token = secret
                        .issue(&user, SESSION_TTL)
                        .map_err(|e| HttpError::Internal(e.to_string()))?;
                    let mut resp = Response::text(StatusCode::OK, user.clone());
                    gate.set_session_cookie(&mut resp, &token)?;
                    Ok(resp)
                }
            })
            .get_session("/me", move |ctx: RequestCtx| {
                let secret = me_secret.clone();
                let name = cookie_name.clone();
                async move {
                    let user = session_user_from_cookie(&name, ctx.headers(), &secret)?;
                    Ok(Response::text(StatusCode::OK, user))
                }
            }),
        whoami_state,
    )
}

#[cfg(test)]
mod b42_tests {
    use super::*;

    #[test]
    fn b42_whoami_path_matches_elastic_lb_and_local_cluster_scripts() {
        assert_eq!(E2E_WHOAMI_PATH, "/e2e/whoami");
    }
}
