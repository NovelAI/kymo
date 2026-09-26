#[tokio::main]
async fn main() -> anyhow::Result<()> {
    kymo_server::run().await
}
