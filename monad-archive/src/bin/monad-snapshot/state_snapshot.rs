use eyre::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Mutex,
};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};

use monad_archive::{kvstore::retry, prelude::*};

const MULTIPART_SIZE: usize = 1024 * 1024 * 100; // 100MB

pub async fn upload_directory(
    dir: PathBuf,
    s3: Client,
    bucket: String,
    concurrency: usize,
) -> Result<()> {
    let mut read_dir = tokio::fs::read_dir(dir).await?;

    // Upload all files in the directory
    // Note: this level is done sequentially under the assumption that each file is large enough to use the full concurrency
    // Revisit this if that assumption is not true
    while let Some(entry) = read_dir.next_entry().await? {
        let path = entry.path();
        let key = path.file_name().unwrap().to_str().unwrap();

        upload_file_multipart(
            s3.clone(),
            bucket.clone(),
            key.to_string(),
            path.clone(),
            concurrency,
        )
        .await
        .wrap_err_with(|| format!("Failed to upload file {}", key))?;
    }

    Ok(())
}

pub async fn download_bucket_to_dir(
    dir: PathBuf,
    s3: Client,
    bucket: String,
    concurrency: usize,
) -> Result<()> {
    let resp = s3.list_objects_v2().bucket(&bucket).send().await?;
    let object_names = resp
        .contents
        .wrap_err("Bucket has no contents")?
        .into_iter()
        .filter_map(|obj| obj.key)
        .collect::<Vec<_>>();

    for object in object_names {
        let path = dir.join(object);
        let key = path.file_name().unwrap().to_str().unwrap();
        download_file_multipart(
            s3.clone(),
            bucket.clone(),
            key.to_string(),
            path.clone(),
            concurrency,
        )
        .await
        .wrap_err_with(|| format!("Failed to download file {}", key))?;
    }

    Ok(())
}

pub async fn download_file_multipart(
    s3: Client,
    bucket: String,
    key: String,
    file_path: PathBuf,
    concurrency: usize,
) -> Result<()> {
    // Create the file
    let file = File::create(&file_path).await.wrap_err_with(|| {
        format!(
            "Failed to create filepath {} for {}",
            file_path.display(),
            key
        )
    })?;

    // Get the object size
    let object_size =
        s3.head_object()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to get object size for {}", key))?
            .content_length()
            .wrap_err_with(|| format!("Failed to get object size for {}", key))? as usize;

    // Generate an iterator of download requests
    let requests = (0..=object_size / MULTIPART_SIZE).map(|i| {
        let offset = i * MULTIPART_SIZE;
        let part_size = MULTIPART_SIZE.min(object_size - offset);
        let part_offset = offset;
        let part_end = part_offset + part_size - 1;
        let range_header_str = format!("bytes {}-{}/{}", part_offset, part_end, object_size);

        s3.get_object()
            .bucket(&bucket)
            .key(&key)
            .range(range_header_str)
    });

    // Download `concurrency` parts in parallel, then write the ordered results to the file
    futures::stream::iter(requests)
        .map(|req| {
            tokio::spawn(async move {
                retry(|| async {
                    let resp = req.clone().send().await.wrap_err("Failed to get part")?;
                    resp.body.collect().await.wrap_err("Failed to collect part")
                })
                .await
            })
        })
        .map(|r| async { r.await.wrap_err("Task join err in download multipart")? })
        .buffered(concurrency)
        .try_fold(file, |mut file, mut body| async move {
            file.write_all_buf(&mut body)
                .await
                .wrap_err_with(|| format!("Failed to write part"))?;
            Ok(file)
        })
        .await
        .wrap_err_with(|| format!("Failed to write file"))?;

    Ok(())
}

pub async fn upload_file_multipart(
    s3: Client,
    bucket: String,
    key: String,
    file_path: PathBuf,
    concurrency: usize,
) -> Result<()> {
    // Create the multipart upload
    let upload = retry(|| async {
        s3.create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to create multipart upload for {}", key))
    })
    .await
    .wrap_err_with(|| format!("Failed to create multipart upload for {}", key))?;
    let upload_id = upload.upload_id().unwrap().to_owned();

    // Open the file
    let file = File::open(&file_path).await.wrap_err_with(|| {
        format!(
            "Failed to open filepath {} for {}",
            file_path.display(),
            key
        )
    })?;
    let file_size = file.metadata().await?.len() as usize;
    let file = Arc::new(Mutex::new(file));

    // Define a stream of upload request tasks
    let requests = futures::stream::iter(0..=file_size / MULTIPART_SIZE).map(|part_number| {
        // Spawn a task to upload a part
        // Note: all these clones are cheap
        tokio::spawn(upload_task(
            file.clone(),
            s3.clone(),
            bucket.clone(),
            key.clone(),
            upload_id.clone(),
            part_number as i32,
        ))
    });

    // Execute request tasks and collect completed parts
    let completed_parts = requests
        .map(|r| async { r.await.wrap_err("Task join err in upload multipart")? })
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await
        .wrap_err("Failed to collect upload parts")?;

    retry(|| async {
        s3.complete_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(completed_parts.clone()))
                    .build(),
            )
            .send()
            .await
    })
    .await?;

    Ok(())
}

async fn upload_task(
    file: Arc<Mutex<File>>,
    s3: Client,
    bucket: String,
    key: String,
    upload_id: String,
    part_number: i32,
) -> Result<CompletedPart> {
    let mut buffer = Vec::with_capacity(MULTIPART_SIZE);
    file.lock()
        .await
        .read(&mut buffer)
        .await
        .wrap_err_with(|| format!("Failed to read file for {}", key))?;
    let buffer = Arc::new(buffer);

    let part_checksum = {
        let buffer = buffer.clone();
        tokio::task::spawn_blocking(move || {
            let mut hash = 0;
            // Use bounded rayon thread pool since tokio's blocking pool is not meant for unbounded
            // number of compute tasks
            rayon::scope(|s| {
                s.spawn(|_| {
                    hash = crc32fast::hash(&buffer);
                });
            });
            assert!(hash != 0, "Control flow misundestood, hash is 0");
            hash
        })
        .await
        .wrap_err("Failed to compute part checksum")?
    };

    let part = retry(|| async {
        s3.upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(&upload_id)
            // Unfortunate clone, consider manual retry impl if this causes perf issues
            .body(ByteStream::from((*buffer).clone()))
            .part_number(part_number)
            .checksum_crc32(part_checksum.to_string())
            .send()
            .await
            .wrap_err_with(|| format!("Failed to upload part {}", part_number))
    })
    .await
    .wrap_err("Failed to upload part")?;

    Ok::<_, Error>(
        CompletedPart::builder()
            .part_number(part_number)
            .e_tag(part.e_tag().unwrap())
            .build(),
    )
}
