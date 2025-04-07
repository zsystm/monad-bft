use eyre::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncWriteExt},
    sync::{mpsc, Mutex},
    task::JoinSet,
};

use aws_sdk_s3::{
    primitives::ByteStream,
    types::{CompletedMultipartUpload, CompletedPart},
    Client,
};

use crate::{kvstore::retry, prelude::*};

const MULTIPART_SIZE: usize = 1024 * 1024 * 100; // 100MB

pub async fn download_file_multipart(
    s3: Client,
    bucket: String,
    key: String,
    file_path: PathBuf,
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
        let range_string = format!("bytes {}-{}/{}", part_offset, part_end, object_size);

        s3.get_object()
            .bucket(&bucket)
            .key(&key)
            .range(range_string)
    });

    // Download `concurrency` parts in parallel, then write the ordered results to the file
    let concurrency = 10; // 10 * MULTIPART_SIZE bytes will be buffered in ram
    futures::stream::iter(requests)
        .map(|req| async {
            tokio::spawn(async move {
                retry(|| async {
                    let resp = req.clone().send().await.wrap_err("Failed to get part")?;
                    resp.body.collect().await.wrap_err("Failed to collect part")
                })
                .await
            })
            .await
            .wrap_err("Failed to join part task")?
        })
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

    // Generate a stream of upload request tasks
    let requests = futures::stream::iter(0..=file_size / MULTIPART_SIZE).map(|part_number| {
        let part_number = part_number as i32;
        let s3 = s3.clone();
        let bucket = bucket.clone();
        let key = key.clone();
        let upload_id = upload_id.clone();
        let file = file.clone();

        async move {
            let mut buffer = Vec::with_capacity(MULTIPART_SIZE);
            file.lock()
                .await
                .read(&mut buffer)
                .await
                .wrap_err_with(|| format!("Failed to read file for {}", key))?;
            let buffer = Arc::new(buffer);

            tokio::spawn(async move {
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
            })
            .await
            .wrap_err("Join err building requests")?
        }
    });

    // Execute request tasks and collect completed parts
    let concurrency = 10;
    let completed_parts = requests
        .buffered(concurrency)
        .try_collect::<Vec<_>>()
        .await
        .wrap_err("Failed to collect parts")?;

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

// async fn read_file_parts(file: &mut File, concurrency: usize) -> Result<mpsc::Receiver<Vec<u8>>> {
//     let (tx, rx) = mpsc::channel(concurrency);

//     let mut buffer = Vec::with_capacity(MULTIPART_SIZE);
//     while let Ok(n) = file.read(&mut buffer).await {
//         if n == 0 {
//             break;
//         }
//         tx.send(buffer).await.wrap_err("Failed to send part")?;
//         buffer = Vec::with_capacity(MULTIPART_SIZE);
//     }

//     Ok(rx)
// }

// async fn upload_file_parts(
//     s3: Client,
//     bucket: String,
//     key: String,
//     mut chunk_rx: mpsc::Receiver<Vec<u8>>,
//     concurrency: usize,
//     upload_id: String,
// ) -> Result<()> {
//     let mut next_part_number = 0;
//     let join_set = JoinSet::new();
//     while let Some(buffer) = chunk_rx.recv().await {
//         join_set.spawn(async move {
//             let part_number = next_part_number;
//             let buffer = Arc::new(buffer);
//             let part_checksum = {
//                 let buffer = buffer.clone();
//                 tokio::task::spawn_blocking(move || {
//                     let mut hash = 0;
//                     // Use bounded rayon thread pool since tokio's blocking pool is not meant for unbounded
//                     // number of compute tasks
//                     rayon::scope(|s| {
//                         s.spawn(|_| {
//                             hash = crc32fast::hash(&buffer);
//                         });
//                     });
//                     assert!(hash != 0, "Control flow misundestood, hash is 0");
//                     hash
//                 })
//                 .await
//                 .wrap_err("Failed to compute part checksum")?
//             };

//             let part = retry(|| async {
//                 s3.upload_part()
//                     .bucket(&bucket)
//                     .key(&key)
//                     .upload_id(&upload_id)
//                     // unfortunate clone, consider manual retry impl if this causes perf issues
//                     .body(ByteStream::from((*buffer).clone()))
//                     .part_number(part_number)
//                     .checksum_crc32(part_checksum.to_string())
//                     .send()
//                     .await
//                     .wrap_err_with(|| format!("Failed to upload part {}", part_number))
//             })
//             .await
//             .wrap_err("Failed to upload part")?;

//             Ok::<_, Error>(
//                 CompletedPart::builder()
//                     .part_number(part_number)
//                     .e_tag(part.e_tag().unwrap())
//                     .build(),
//             )
//         })
//     }
//     join_set.join
// }
