//! CLI for product showcase HTTP gateways (`publish = false`).

use crafty_showcase_client::{
    ClientError, cast_actor, enqueue_job, resume_workflow, run_workflow, ws_chat,
};

fn usage() -> ! {
    eprintln!(
        "usage:
  crafty-showcase-client job <gateway> <stream> <payload>
  crafty-showcase-client cast <gateway> <group> <payload>
  crafty-showcase-client workflow run <trigger> <saga-id>
  crafty-showcase-client workflow resume <trigger> <saga-id>
  crafty-showcase-client ws <gateway> <user> <message>"
    );
    std::process::exit(2);
}

fn print_resp(label: &str, resp: &crafty_showcase_client::HttpResponse) {
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
        _ => usage(),
    }
    Ok(())
}
