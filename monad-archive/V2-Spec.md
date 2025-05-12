## Problem

The current archive data model was built around using S3 object storage as the primary collection keyed by block_number and a cloud kv-store keyed by tx_hash. This simple model is easy to reason about and fits well with near-infinitely scalable cloud services with high round-trip latencies. However, when used as the primary data source for serving rpc requests, this model breaks down in the following ways:

- Cloud instances have high read latency due to being geographically distant from many rpc servers. 100-400 ms request latency is common when pulling from S3 for our Lithuania nodes, Asia latency is likely even worse.
- Egress bandwidth is expensive and necessary since RPC Fullnodes are supposed to run on bare-metal, not aws ec2. This also lower bounds latency even if we ran multiple geographically distributed cloud data stores
- The self-hosted archive implemented on top of MongoDB solves the latency issue, but the current kv-store based implementation does not fit well with the constraints of a self-hosted local db
    - Limited drive size (e.x. 32Tb) means only recent data can be stored without sharding and multi-node clusters
        - At 10k tps with current denormalized datamodel, ~3.3TB would be produced every day. This means only ~9 days of history on a 32TB drive (See [Appendix A: Data scale](https://www.notion.so/Appendix-A-Data-scale-1e075b0ba840819abbdde407391c52ba?pvs=21))
    - There is no way to store the full archive since sharding is not easily supported with how the current abstraction layers are defined in code.
    - De-normalized data is not necessary since MongoDB supports a more expressive query language and rpc ↔ db latency is closer to 5ms than 100-400ms.
    - tx_hash data in the kv model is `tx_hash ⇒ rlp(tx_body, receipt, trace, header_subset)` which means unnecessary data is returned by most queries. Trace data is larger than tx_body or receipts, yet it is queried the least frequently.

## Proposal Draft 2:

The archive system can be thought of in two pieces

1. Storing the raw blockchain data, principally block headers along with transaction bodies, receipts and traces
2. Maintaining primary key and secondary indexes to support RPC queries

Currently, archive each backend implements both pieces and uses the same kv-store interface to do so. Specifically, aws uses s3 + dynamodb where dynamodb exists solely to support (2) and additionally has significant data duplication to reduce round trips. The self-hosted / mongo backend uses the same kv-stores, keys, etc. and therefore has the same duplication and structures, even though they are not optimal for the performance profile of that database type and deployment. 

In practice we see that nearly all operators that need (2), namely RPC, indexer and sub-graph providers, use the self-hosted archive / mongo backend almost exclusively and rarely use the aws implementation to serve rpc requests. This means:

- AWS principally serves (1) storing raw data reliably and scalably
- Self-Hosted Archive serves both (1) storing data and (2) structuring indexes and keys to performantly answer RPC queries

With this in mind, along with the other considerations from the “Problems” section, we propose migrating to one implementation to support rpc queries backed by MongoDB, with the option for old data to be evicted into S3 cold storage, but still indexed by MongoDB. This allows us to use 1 direct, conceptually simpler and more optimized implementation to support the 3 main deployment patterns we care about:

- Cloud hosted, managed “forever” archive
    - Use MongoDB Atlas, optionally with S3 cold storage to mitigate costs
    - Run by Category Labs, Monad Foundation and possibly other archiving entities
- Self-hosted, full history from-genesis, runs colocated to bare-metal RPC servers
    - Use sharded MongoDB cluster to horizontally scale
    - Mainly aimed towards professional RPC providers
- Self-hosted, recent history stored locally, runs colocated to bare-metal RPC servers
    - Use single-node MongoDB with old, infrequently accessed blob data evicted to S3 cold storage
    - Aimed towards less sophisticated 3rd parties that don’t want the operational complexity and/or expense of running a clustered db. i.e. “`docker compose up` is all you need”

The paradigm would allow us to remove AWS DynamoDB, the API Gateway, CloudProxy Lambda, API Key table and other miscellaneous services supporting (2) for the AWS backend. Additionally we would not need to run nor maintain separate indexer processes and the archiver codebase can be simplified significantly. 

Importantly, unlike the original version of the Self-Hosted Archive V2 proposal, we would retain **one, unified data model**. 

### Data Model

**Goals**: 

- Table and schema structures tailored to MongoDB performance characteristics
    - Normalized MongoDB data model allows more data to be local
    - **~2X** more blocks available for same size drive: 19 days instead of 9 at 10k tps, or 38 days if traces are offloaded to S3 (See [Appendix A: Data scale](https://www.notion.so/Appendix-A-Data-scale-1e075b0ba840819abbdde407391c52ba?pvs=21) )
- Support for both
    - full-history self-hosted MongoDB sharded clusters.
    - full-history managed MongoDb sharded clusters (MongoDb Atlas)
    - operationally simple single-node MongoDB setups, but with only recent data
    - This flexibility allows RPC operators to trade off between operational simplicity, cost, performance and full-self-hosting all while using the same codebase
- More efficient and maintainable `eth_getLogs` indexed queries than is possible to bolt onto the current data model

See [Appendix B: Other Database Options](https://www.notion.so/Appendix-B-Other-Database-Options-1e075b0ba8408112ad37d469cc6b58b9?pvs=21) for why MongoDB was chosen

**Collections**:

1. **blocks**:
    - _id: block_number
    - Fields: hash, header_data, tx_hashes (array)
    - Indexes: _id, hash (unique)
    - Sharding: [shard_prefix, _id *(*block_number)]
        - `shard_prefix = block_number / 10 % shards`
        - Prefix details explained
            
            Native hashed sharding could also be used, but then block range queries are never colocated and always require scatter/get pattern. 
            
            Using this prefix means that blocks are still colocated in groups of 10, but a shard is not “hot” for more than 10 blocks before rotating. This does reduce write latency, but that's fine for the archive use case, and with buffering this shouldn’t reduce total throughput. This would allow for more efficient range scans
            
            This is a small detail, so exactly which final approach is chosen can be determined empirically 
            
2. **txs**:
    - _id: tx_hash
    - Fields:
        - block_number
        - block_hash
        - tx_body: Binary OR binary_offset to s3/object-storage
        - receipt: Binary OR binary_offset to s3/object-storage
        - trace: Binary OR binary_offset to s3/object-storage
    - Indexes:
        - _id: tx_hash
        - block_number
        - logs: nested array of [address 4-byte prefix, topic_[0-3] 4-byte prefix]
            - Used for eth_getLogs query acceleration
            - Note: exact indexes for eth_getLogs still under investigation. Other options include
                - storing a per-tx bloom filter of all logs and using bitwise operations
                - first filtering blocks by bloom filter in block header, then using above index for txs
    - Sharding: _id (tx_hash)

**Query Patterns:**

- tx based rpc methods,
    - ex: `eth_getTransactionReceipt(tx_hash)`
    - Fast point lookup on transactions by _id.
    - Returns document with inline txbody or receipt. If offset returned for old data, lookup in S3 Object Storage
- Block number or block hash based rpc methods
    - ex. `eth_getReceiptsByBlock(num)`
    - Read blocks collection by _id: num (gets header & tx_hashes).
    - Perform a multi-get (`find({ _id: { $in: tx_hashes } }, { projection: { receipt: 1 } })`) on the transactions collection. This returns all projected receipts.  Performance generally good, but depends on Mongo's multi-get efficiency across shards. If offset returned for old data, lookup in S3 Object Storage
- Trace methods:
    - Query like above. If offset returned for old data, lookup in S3 Object Storage
    - Could optionally store traces inline or using a non-s3 object store, but default to S3 since traces are larger than tx_bodies or receipts, but accessed less frequently
- eth_getLogs
    - Multiple possible approaches, but current default is:
        - Take the block range and 4-byte prefixes of query address(es) and topic(s) and run a query of the following form:
        
        ```jsx
        find({
        	block_number:  { "$gte": 10000, "$lte": 20000 }, 
        	"logs.address": {"$in": [Binary.createFromBase64('8wIhcA==', 0)]},
        	"logs.topic_0": {"$in": [Binary.createFromBase64('1xIacA==', 0), Binary.createFromBase64('5aEaBC==', 0)]}
        	// ... for topics 1-3 if present in query
        }
        ```
        
        - Matching entries are projected to `{block_number, tx_body, receipt}`
        - Use `$lookup` aggregation to join with block headers
        - Return to rpc server and run full filtering (4-byte prefixes provide approximately <1% false positive rate according to earlier calculations, but this can vary)
        - Construct rpc response using the same code as currently runs
    - This enables much larger block ranges compared to current implementation if query params filter out a high percentage of logs

## Implementation Plan

**Code Changes**
\<redacted\>

**Operational**

1. Backfill a MongoDB instance from S3 data
2. Connect RPC to MongoDB instance 
3. Write operational docs for above
4. Benchmark common queries end-to-end
5. Set up sharded MongoDB cluster and document process 

**Timeline Estimate:**

- 1-2 weeks: core code changes
    - At this stage we can do unit and PoC testing
- 1 week: ancillary code changes, rpc server integration, cleanup, unused feature removal etc.
- 1-4 weeks: testing, operational rollout, changing metrics/monitoring, documentation writing, fixing bugs, migrating from old system to new system (likely running both for a period of time for redundancy)

### Appendix A: Data scale

Sampled from 100k block range: 11,054,196 - 11,154,196

Total txs: 13,250,000 ⇒ ~133 tps

|  | Total kb | Kb per tx | gb/day at 10k tps | gb/day at 10ks (current denormalized) |
| --- | --- | --- | --- | --- |
| Blocks (body + header) | 3,980,000 | 0.3 | 250 | ~500 |
| Receipts | 7,708,000 | 0.55 | 450 | ~900 |
| Traces | 15,200,000 | 1.15 | 950 | ~1900 |
| Total | 26,888 | 2 | 1,650 | ~3,330 |

**Days of storage for 32TB drive with normalized data model (x2 for current denormalized)**

|  | 10k tps (days) | 3k tps (days) |
| --- | --- | --- |
| All data local | 19 | 64 |
| Traces in S3 | 38 | 126 |
| Traces and Receipts in S3 | 123 | 410 |
| Only txhash ⇒ block_num and block headers local | 614 | 2046 |

## Appendix B: Other Database Options

Three flavors of database were considered for the Self-Hosted Archive V2 

- Wide-column, e.g. Cassandra/ScyllaDB
- Document, e.g. MongoDB
- SQL, e.g. Postgres

Whichever database is chosen should have the following 4 properties:

1. Native sharding support for infinite horizontal write scaling 
2. High throughput write-once data ingest
3. Secondary indices to enable eth_getLogs w/o a separate db (i.e. reduces system complexity)
4. Good support for multiple deployment models: single node, self-hosted sharded, managed cloud

**SQL**

Postgres can be eliminated quickly since we do not need strong transactional guarantees or sophisticated relational queries, but do need very high ingest rates and operationally simple horizontal scaling through write sharding. 

*Verdict: Fails #1, Partial #2*

**Wide-column**

ScyllaDB provides a natural fit for archive data in most respects: high write-once ingest volume, native efficient sharding, high performance kv-reads. The main issue is supporting eth_getLogs queries efficiently without more expressive index options. Realistically this would have to be offloaded to a secondary db, which complicates operations and implementation significantly. 

ScyllaDB must *always* be run in cluster mode, increasing the minimum operational complexity. 

*Verdict: Fails #3 and #4*

**Document**

Mongo provides native sharding support and scalable high ingest rate while also allowing expressive secondary indexes to support eth_getLogs acceleration without another db. 

Mongo also runs perfectly well as a single, non-sharded db which is easy to set up and run for small operators that wish to keep a local cache of recent historical data (on the order of 1-3 months) and are fine with falling back to cloud storage for older “frozen” data that is less likely to be read frequently. This flexibility is important as it allows small fullnode operators to choose what tradeoff between complexity, cost and performance best fits their needs, while still being able to use the same codebase.

*Verdict: Satisfies all*

- Draft 1
    
    ## Problem
    
    The current archive data model was built around using S3 object storage as the primary collection keyed by block_number and a cloud kv-store keyed by tx_hash. This simple model is easy to reason about and fits well with near-infinitely scalable cloud services with high round-trip latencies. However, when used as the primary data source for serving rpc requests, this model breaks down in the following ways:
    
    - Cloud instances have high read latency due to being geographically distant from many rpc servers. 100-400 ms request latency is common when pulling from S3 for our Lithuania nodes, Asia latency is likely even worse.
    - Egress bandwidth is expensive and necessary since RPC Fullnodes are supposed to run on bare-metal, not aws ec2. This also lower bounds latency even if we ran multiple geographically distributed cloud data stores
    - The self-hosted archive implemented on top of MongoDB solves the latency issue, but the current kv-store based implementation does not fit well with the constraints of a self-hosted local db
        - Limited drive size (e.x. 32Tb) means only recent data can be stored without sharding and multi-node clusters
            - At 10k tps with current denormalized datamodel, ~3.3TB would be produced every day. This means only ~9 days of history on a 32TB drive (See [Appendix A: Data scale](https://www.notion.so/Appendix-A-Data-scale-1d675b0ba840802ead51ee0ccaef0549?pvs=21))
        - There is no way to store the full archive since sharding is not easily supported with how the current abstraction layers are defined in code.
        - De-normalized data is not necessary since MongoDB supports a more expressive query language and rpc ↔ db latency is closer to 5ms than 100-400ms.
        - tx_hash data in the kv model is `tx_hash ⇒ rlp(tx_body, receipt, trace, header_subset)` which means unnecessary data is returned by most queries. Trace data is larger than tx_body or receipts, yet it is queried the least frequently.
    
    ## Proposal
    
    Write a MongoDB specific archive backend instead of using the same `KVStore` trait for both AWS and self-hosted archive. This allows:
    
    - Different table and schema structures tailored to AWS vs MongoDB performance characteristics (details below)
        - Normalized MongoDB data model allows more data to be local
        - **~2X** more blocks available for same size drive: 19 days instead of 9 at 10k tps, or 38 days if traces are offloaded to S3 (See [Appendix A: Data scale](https://www.notion.so/Appendix-A-Data-scale-1d675b0ba840802ead51ee0ccaef0549?pvs=21) )
    - Support for both
        - full-history self-hosted MongoDB sharded clusters.
        - full-history managed MongoDb sharded clusters (MongoDb Atlas)
        - operationally simple single-node MongoDB setups, but with only recent data
        - This flexibility allows RPC operators to trade off between operational simplicity, cost, performance and full-self-hosting all while using the same codebase
    - More efficient and maintainable `eth_getLogs` indexed queries than is possible to bolt onto the current data model
    
    See [Appendix B: Other Database Options](https://www.notion.so/Appendix-B-Other-Database-Options-1d675b0ba84080348147c091568085b4?pvs=21) for why MongoDB was chosen
    
    ### Proposed MongoDB Data Model
    
    ***Note:*** This is a **starting point** not the final data model. It represents a clear improvement over the existing kv-based data model, but the main benefit is a more direct in-code implementation that allows experimentation and mongo-specific optimization over time. 
    
    **Collections**:
    
    1. **blocks**:
        - _id: block_number
        - Fields: hash, header_data, tx_hashes (array)
        - Indexes: _id, hash (unique)
        - Sharding: [shard_prefix, _id *(*block_number)]
            - `shard_prefix = block_number / 10 % shards`
            - Prefix details explained
                
                Native hashed sharding could also be used, but then block range queries are never colocated and always require scatter/get pattern. 
                
                Using this prefix means that blocks are still colocated in groups of 10, but a shard is not “hot” for more than 10 blocks before rotating. This does reduce write latency, but that's fine for the archive use case, and with buffering this shouldn’t reduce total throughput. This would allow for more efficient range scans
                
                This is a small detail, so exactly which final approach is chosen can be determined empirically 
                
    2. **txs**:
        - _id: tx_hash
        - Fields:
            - block_number
            - body: Binary (inline tx body)
            - receipt: Binary (Inline receipt data)
            - trace_loc: binary_offset to s3/object-storage
        - Indexes:
            - _id: tx_hash
            - block_number
            - logs: nested array of [address 4-byte prefix, topic_[0-3] 4-byte prefix]
                - Used for eth_getLogs query acceleration
                - Note: exact indexes for eth_getLogs still under investigation. Other options include
                    - storing a per-tx bloom filter of all logs and using bitwise operations
                    - first filtering blocks by bloom filter in block header, then using above index for txs
        - Sharding: _id (tx_hash)
            - Alternatively: [shard_prefix, block_number] so that headers and txs are colocated in the same shard and block-level methods like getBlockReceipts are faster. This would make tx-level methods require scatter/get though, so a txhash → block mapping may be required if scatter/get proves too slow.
            - Tradeoff:
                - shard(tx_hash) is good for tx-level methods, but makes block-level methods slower
                - shard([shard_prefix, block_number]) is good for block-level methods, but requires scatter/get for tx-level, or 2 phased txhash→ block_number lookup.
            - Most likely shard(tx_hash) is best as a starting point
    
    **Query Patterns:**
    
    - tx based rpc methods,
        - ex: `eth_getTransactionReceipt(tx_hash)`
        - Fast point lookup on transactions by _id.
        - Returns document with inline txbody or receipt
    - Block number or block hash based rpc methods
        - ex. `eth_getReceiptsByBlock(num)`
        - Read blocks collection by _id: num (gets header & tx_hashes).
        - Perform a multi-get (`find({ _id: { $in: tx_hashes } }, { projection: { receipt: 1 } })`) on the transactions collection. This returns all projected receipts.  Performance generally good, but depends on Mongo's multi-get efficiency across shards.
    - Trace methods:
        - Query like above, then use fetch from s3
        - Could optionally store traces inline or using a non-s3 object store, but default to S3 since traces are larger than tx_bodies or receipts, but accessed less frequently
    - eth_getLogs
        - Multiple possible approaches, but current default is:
            - Take the block range and 4-byte prefixes of query address(es) and topic(s) and run a query of the following form:
            
            ```jsx
            find({
            	block_number:  { "$gte": 10000, "$lte": 20000 }, 
            	"logs.address": {"$in": [Binary.createFromBase64('8wIhcA==', 0)]},
            	"logs.topic_0": {"$in": [Binary.createFromBase64('1xIacA==', 0), Binary.createFromBase64('5aEaBC==', 0)]}
            	// ... for topics 1-3 if present in query
            }
            ```
            
            - Matching entries are projected to `{block_number, tx_body, receipt}`
            - Use `$lookup` aggregation to join with block headers
            - Return to rpc server and run full filtering (4-byte prefixes provide approximately <1% false positive rate according to earlier calculations, but this can vary)
            - Construct rpc response using the same code as currently runs
        - This enables much larger block ranges compared to current implementation if query params filter out a high percentage of logs
    
    ## Implementation Plan
    
    **Code Changes**
    
    1. Define the above data model in rust code
        1. In module test against mongo running in docker
    2. Implement BlockDataWriter/Reader and TxIndexWriter/Reader traits 
    3. Update plumbing to construct and use new impls (low effort)
    4. Simple benchmarks against S3/DynamoDB, old MongoDB and new MongoDB backends
    5. Remove MongoDB KVStore impl 
    6. Flexnet integration tests
    7. Write job to prune oldest data to stay within disk capacity, replaces use of capped collections feature
    
    **Operational**
    
    1. Backfill a MongoDB instance from S3 data
    2. Connect RPC to MongoDB instance 
    3. Write operational docs for above
    4. Benchmark common queries end-to-end
    5. Set up sharded MongoDB cluster and document process 
    
    ### Appendix A: Data scale
    
    Sampled from 100k block range: 11,054,196 - 11,154,196
    
    Total txs: 13,250,000 ⇒ ~133 tps
    
    |  | Total kb | Kb per tx | gb/day at 10k tps | gb/day at 10ks (current denormalized) |
    | --- | --- | --- | --- | --- |
    | Blocks (body + header) | 3,980,000 | 0.3 | 250 | ~500 |
    | Receipts | 7,708,000 | 0.55 | 450 | ~900 |
    | Traces | 15,200,000 | 1.15 | 950 | ~1900 |
    | Total | 26,888 | 2 | 1,650 | ~3,330 |
    
    **Days of storage for 32TB drive with normalized data model (x2 for current denormalized)**
    
    |  | 10k tps (days) | 3k tps (days) |
    | --- | --- | --- |
    | All data local | 19 | 64 |
    | Traces in S3 | 38 | 126 |
    | Traces and Receipts in S3 | 123 | 410 |
    | Only txhash ⇒ block_num and block headers local | 614 | 2046 |
    
    ## Appendix B: Other Database Options
    
    Three flavors of database were considered for the Self-Hosted Archive V2 
    
    - Wide-column, e.g. Cassandra/ScyllaDB
    - Document, e.g. MongoDB
    - SQL, e.g. Postgres
    
    **SQL**
    
    Postgres can be eliminated quickly since we do not need strong transactional guarantees or sophisticated relational queries, but do need very high ingest rates and operationally simple horizontal scaling through write sharding.
    
    **Wide-column**
    
    ScyllaDB provides a natural fit for archive data in most respects: high write-once ingest volume, native efficient sharding, high performance kv-reads. The main issue is supporting eth_getLogs queries efficiently without more expressive index options. Realistically this would have to be offloaded to a secondary db, which complicates operations and implementation significantly. 
    
    ScyllaDB must *always* be run in cluster mode, increasing the minimum operational complexity.
    
    **Document**
    
    Mongo provides native sharding support and scalable high ingest rate while also allowing expressive secondary indexes to support eth_getLogs acceleration without another db. 
    
    Mongo also runs perfectly well as a single, non-sharded db which is easy to set up and run for small operators that wish to keep a local cache of recent historical data (on the order of 1-3 months) and are fine with falling back to cloud storage for older “frozen” data that is less likely to be read frequently. This flexibility is important as it allows small fullnode operators to choose what tradeoff between complexity, cost and performance best fits their needs, while still being able to use the same codebase.
