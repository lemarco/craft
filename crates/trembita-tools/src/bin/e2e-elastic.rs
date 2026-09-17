//! Product cluster binary for elastic join + LB E2E ([`e2e/elastic_lb.sh`](../../../e2e/elastic_lb.sh)).

use trembita_tools::e2e_elastic;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    e2e_elastic::server_builder()?.run().await?;
    Ok(())
}
