use aws_sdk_s3::Client;
use clap::Parser;
use monad_archive::{cli::get_aws_config, prelude::*};
use state_snapshot::{download_bucket_to_dir, upload_directory};

mod state_snapshot;

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let s3 = Client::new(&get_aws_config(Some(args.region)).await);

    match args.subcommand {
        Subcommand::Upload(upload_args) => {
            upload_directory(args.path, s3, args.bucket, args.concurrency).await?;
        }
        Subcommand::Download(download_args) => {
            download_bucket_to_dir(args.path, s3, args.bucket, args.concurrency).await?;
        }
        Subcommand::UploadService(upload_service_args) => {
            let metrics = Metrics::new(
                upload_service_args.otel_endpoint,
                "snapshot_upload_service".to_string(),
                upload_service_args.otel_replica_name,
                Duration::from_secs(upload_service_args.period_secs),
            )?;
            upload_service(
                args.path,
                s3,
                args.bucket,
                args.concurrency,
                upload_service_args.period_secs,
                metrics,
            )
            .await?;
        }
    }
    Ok(())
}

async fn upload_service(
    path: PathBuf,
    s3: Client,
    bucket: String,
    concurrency: usize,
    period_secs: u64,
    metrics: Metrics,
) -> Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(period_secs));
    loop {
        let now = interval.tick().await;
        metrics.inc_counter("snapshot_upload_service_upload_started_count");
        info!("Uploading snapshot to S3");

        upload_directory(path.clone(), s3.clone(), bucket.clone(), concurrency).await?;

        info!(
            "Snapshot uploaded to S3, sleeping for {} seconds...",
            period_secs
        );
        metrics.inc_counter("snapshot_upload_service_upload_completed_count");
        metrics.gauge(
            "snapshot_upload_service_upload_duration_secs",
            now.elapsed().as_secs_f64().round() as u64,
        );
    }
}

#[derive(Debug, clap::Parser)]
struct Args {
    #[clap(subcommand)]
    subcommand: Subcommand,

    #[clap(short, long, global = true)]
    bucket: String,

    #[clap(short, long, global = true)]
    path: PathBuf,

    #[clap(short, long, global = true)]
    concurrency: usize,

    #[clap(short, long, global = true)]
    region: String,
}

#[derive(Debug, clap::Subcommand)]
enum Subcommand {
    UploadService(UploadServiceArgs),
    Upload(UploadArgs),
    Download(DownloadArgs),
}

#[derive(Debug, clap::Args)]
struct UploadArgs {}

#[derive(Debug, clap::Args)]
struct DownloadArgs {}

#[derive(Debug, clap::Args)]
struct UploadServiceArgs {
    #[clap(long)]
    period_secs: u64,

    #[clap(long)]
    otel_endpoint: Option<String>,

    #[clap(long)]
    otel_replica_name: String,
}
