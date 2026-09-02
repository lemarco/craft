//! CLI for product showcase HTTP gateways (`publish = false`).

use trembita_tools::showcase_client::{
    ClientError, cast_actor, enqueue_job, post_chat, post_chat_bearer, resume_workflow,
    run_workflow, submit_order_auth, ws_chat,
};

fn usage() -> ! {
    eprintln!(
        "usage:
  trembita-showcase-client job <gateway> <stream> <payload>
  trembita-showcase-client cast <gateway> <group> <payload>
  trembita-showcase-client workflow run <trigger> <saga-id>
  trembita-showcase-client workflow resume <trigger> <saga-id>
  trembita-showcase-client chat <gateway> <user> <message> [token]
  trembita-showcase-client chat-bearer <gateway> <user> <message> <bearer>
  trembita-showcase-client submit <gateway> <tenant> <order-id> [token]
  trembita-showcase-client ws <gateway> <user> <message>"
    );
    std::process::exit(2);
}

fn print_resp(label: &str, resp: &trembita_tools::showcase_client::HttpResponse) {
    println!("{label} → HTTP {}", resp.status);
    let body = String::from_utf8_lossy(resp.body());
    if !body.trim().is_empty() {
        println!("{}", body.trim());
    }
}

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let mut args = std::env::args().skip(1);
    let cmd = args.next().unwrap_or_else(|| usage());
    match cmd.as_str() {
        "job" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let stream = args.next().unwrap_or_else(|| usage());
            let payload = args.next().unwrap_or_else(|| usage());
            print_resp("enqueue", &enqueue_job(&gateway, &stream, &payload).await?);
        }
        "cast" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let group = args.next().unwrap_or_else(|| usage());
            let payload = args.next().unwrap_or_else(|| usage());
            print_resp("cast", &cast_actor(&gateway, &group, &payload).await?);
        }
        "workflow" => {
            let action = args.next().unwrap_or_else(|| usage());
            let trigger = args.next().unwrap_or_else(|| usage());
            let saga_id = args.next().unwrap_or_else(|| usage());
            let resp = match action.as_str() {
                "run" => run_workflow(&trigger, &saga_id).await?,
                "resume" => resume_workflow(&trigger, &saga_id).await?,
                _ => usage(),
            };
            print_resp("workflow", &resp);
        }
        "ws" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let user = args.next().unwrap_or_else(|| usage());
            let message = args.next().unwrap_or_else(|| usage());
            let reply = ws_chat(&gateway, &user, &message).await?;
            println!("ws → {reply}");
        }
        "chat" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let user = args.next().unwrap_or_else(|| usage());
            let message = args.next().unwrap_or_else(|| usage());
            let token = args.next();
            print_resp(
                "chat",
                &post_chat(&gateway, &user, &message, token.as_deref()).await?,
            );
        }
        "chat-bearer" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let user = args.next().unwrap_or_else(|| usage());
            let message = args.next().unwrap_or_else(|| usage());
            let bearer = args.next().unwrap_or_else(|| usage());
            print_resp(
                "chat",
                &post_chat_bearer(&gateway, &user, &message, &bearer).await?,
            );
        }
        "submit" => {
            let gateway = args.next().unwrap_or_else(|| usage());
            let tenant = args.next().unwrap_or_else(|| usage());
            let order_id: u64 = args
                .next()
                .unwrap_or_else(|| usage())
                .parse()
                .unwrap_or_else(|_| usage());
            let token = args.next();
            print_resp(
                "submit",
                &submit_order_auth(&gateway, &tenant, order_id, token.as_deref()).await?,
            );
        }
        _ => usage(),
    }
    Ok(())
}
