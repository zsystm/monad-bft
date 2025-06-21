use std::{future::Future, pin::Pin, sync::Mutex};
pub use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    ops::RangeInclusive,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

pub use alloy_consensus::{BlockBody, Header, ReceiptEnvelope, ReceiptWithBloom};
pub use alloy_primitives::{U128, U256, U64};
pub use eyre::{bail, eyre, Context, ContextCompat, Result};
pub use futures::{try_join, StreamExt, TryStream, TryStreamExt};
pub use monad_triedb_utils::triedb_env::{ReceiptWithLogIndex, TxEnvelopeWithSender};
use tokio::sync::mpsc;
pub use tokio::time::sleep;
pub use tracing::{debug, error, info, warn, Level};

pub use crate::{
    archive_reader::{ArchiveReader, LatestKind},
    kvstore::{
        dynamodb::DynamoDBArchive, s3::S3Bucket, triedb_reader::TriedbReader, KVReader,
        KVReaderErased, KVStore, KVStoreErased,
    },
    metrics::{MetricNames, Metrics},
    model::{
        block_data_archive::*, tx_index_archive::*, BlockArchiver, BlockDataReader,
        BlockDataReaderErased, HeaderSubset, TxByteOffsets, TxIndexedData,
    },
    model_v2::ModelV2,
};

pub async fn spawn_eyre<T: Future<Output = Result<O>> + Send + 'static, O: Send + 'static>(
    req: T,
    msg: &'static str,
) -> Result<O> {
    let x = tokio::spawn(req).await.wrap_err(msg)?;
    x.wrap_err(msg)
}

#[derive(Debug)]
struct MyError;

impl std::error::Error for MyError {}
impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "E")
    }
}
// type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send>>;
// umbrella trait for Send + 'static
trait SS: Send + 'static {}
impl<T: Send + 'static> SS for T {}

// DIVIDER

// type OpFuture<O> = Pin<Box<dyn Future<Output = Result<O>> + Send>>;
type OpFuture<O> = Pin<Box<dyn Future<Output = Result<O>> + Send>>;
// type OpFactory<O, T: Clone + SS> = Box<dyn Fn(T) -> OpFuture<O> + Send + 'static>;

type ErasedOp = Pin<Box<dyn Future<Output = ()> + Send>>;
// type Op<R, X, O> = Box<dyn Fn(R, X) -> OpFuture<O> + Send + 'static>;
// type Op<R, X, O> = impl Fn(R, X) -> OpFuture<O> + Send + 'static;

fn erase_op<O: Send + 'static>(
    op: impl FnOnce() -> OpFuture<O> + Send + 'static,
) -> (ErasedOp, mpsc::Receiver<Result<O>>) {
    let (tx, rx) = mpsc::channel(1);
    (
        Box::pin(async move {
            let res = op().await;
            tx.send(res).await.expect("Failed to send op result");
        }),
        rx,
    )
}

async fn run_local<R: Clone + SS, O: SS, Item: SS>(
    resource: R,
    // op: Op<R, Item, O>,
    op: impl Fn(R, Item) -> Pin<Box<dyn Future<Output = Result<O, eyre::Report>> + Send>>,
    buf: impl IntoIterator<Item = Item>,
) -> Vec<Result<O>> {
    let buf = buf.into_iter();
    let len = buf.size_hint().0;

    let ops = buf.into_iter().map(|i| {
        let r = resource.clone();
        op(r, i)
    });

    futures::stream::iter(ops)
        // .map(|op| async move { tokio::spawn(op).await.unwrap() })
        .map(|op| async move {
            let res = tokio::spawn(op).await;
            match res {
                Ok(res) => res,
                Err(e) => Err(eyre::Report::from(e).wrap_err("Failed to join tokio task for op")),
            }
        })
        .buffered(len)
        .collect::<Vec<_>>()
        .await
}

async fn run<R: Clone + SS, O: SS, Item: SS>(
    resource: R,
    // op: Op<R, Item, O>,
    op: impl Fn(R, Item) -> OpFuture<O> + Send + 'static,
    buf: impl IntoIterator<Item = Item>,
    submitter: mpsc::Sender<ErasedOp>,
) -> Vec<Result<O>> {
    let buf = buf.into_iter();
    let len = buf.size_hint().0;
    let (tx, mut rx) = mpsc::channel(len);
    for i in buf {
        let r = resource.clone();
        let fut = op(r, i);
        let tx = tx.clone();
        let fut = Box::pin(async move {
            let res = fut.await;
            tx.send(res).await.expect("Failed to send op result");
        });
        submitter.send(fut).await.expect("Failed to send op");
    }

    let mut results = Vec::with_capacity(len);
    while let Some(res) = rx.recv().await {
        results.push(res);
    }
    results
}

struct Exec {
    rx: mpsc::Receiver<ErasedOp>,
    concurrency: usize,
}

impl Exec {
    fn new(concurrency: usize) -> mpsc::Sender<ErasedOp> {
        let (tx, rx) = mpsc::channel(concurrency);
        let exec = Self { rx, concurrency };
        tokio::spawn(async move { exec.run().await });
        tx
    }

    async fn run(mut self) {
        // let mut set = FuturesOrdered::new();
        // futures::stream::iter([0,1,2]).buffered(1)
        while let Some(op) = self.rx.recv().await {
            op.await;
        }
    }
}

struct ParMap<I: Iterator, R, O> {
    iter: I,
    resource: R,
    // op: Op<R, Item, O>,
    op: Box<dyn Fn(R, I::Item) -> Pin<Box<dyn Future<Output = Result<O, eyre::Report>> + Send>>>,
    concurrency: usize,
}

impl<I, R, O> ParMap<I, R, O>
where
    I: Iterator,
    I::Item: SS,
    R: SS + Clone,
    O: SS,
{
    async fn collect_to_vec(self) -> Vec<Result<O>> {
        run_local(self.resource, self.op, self.iter).await
    }

    async fn iter(self) -> impl Iterator<Item = Result<O>> {
        let len = self.iter.size_hint().0;
        let ops = self.iter.map(|i| {
            let r = self.resource.clone();
            (self.op)(r, i)
        });
        futures::stream::iter(ops)
            .buffered(self.concurrency)
            .collect::<Vec<_>>()
            .await
            .into_iter()
    }
}

trait IterParMapExt: Iterator {
    async fn par_async<R, O, F>(
        self,
        resource: &R,
        concurrency: usize,
        op: F,
    ) -> impl Iterator<Item = Result<O>>
    where
        Self: Sized,
        Self::Item: SS,
        O: SS,
        R: SS + Clone,
        F: Fn(R, Self::Item) -> Pin<Box<dyn Future<Output = Result<O, eyre::Report>> + Send>>
            + Send
            + 'static,
    {
        ParMap {
            iter: self,
            resource: resource.clone(),
            op: Box::new(op),
            concurrency,
        }
        .iter()
        .await
    }

    fn par_map<R, O, F>(self, resource: R, concurrency: usize, op: F) -> ParMap<Self, R, O>
    where
        Self: Sized,
        F: Fn(R, Self::Item) -> Pin<Box<dyn Future<Output = Result<O, eyre::Report>> + Send>>
            + Send
            + 'static,
    {
        ParMap {
            iter: self,
            resource,
            op: Box::new(op),
            concurrency,
        }
    }
}

impl<I: Iterator> IterParMapExt for I {}

async fn example() {
    let submitter = Exec::new(10);

    let resource = Arc::new(Mutex::new(vec![1, 2, 3]));
    let data = vec![56, 78, 90];

    let x = data
        .clone()
        .into_iter()
        .par_async(&resource, 10, |r, x| {
            Box::pin(async move {
                let mut r = r.lock().unwrap();
                r.push(x);
                r.push(x);
                Ok(r.len())
            })
        })
        .await
        .collect::<Result<Vec<_>>>()
        .unwrap();

    let results = run_local(
        resource.clone(),
        |r, x| {
            Box::pin(async move {
                let mut r = r.lock().unwrap();
                r.push(x);
                r.push(x);
                r.push(x);
                Ok(())
            })
        },
        data.clone(),
    )
    .await;
    println!("{:?}", results);

    let results = run(
        resource,
        |r, x| {
            Box::pin(async move {
                let mut r = r.lock().unwrap();
                r.push(x);
                r.push(x);
                r.push(x);
                Ok(())
            })
        },
        data,
        submitter,
    )
    .await;
    println!("{:?}", results);
}
