import sys
from log import Log


def compute_tx_per_second(log: Log):
    ledger_commit_df = log.ledger_commit_df()
    tx_per_second = ledger_commit_df["num_tx"].resample("s").sum()
    return tx_per_second


def compute_blocks_per_second(log: Log):
    ledger_commit_df = log.ledger_commit_df()
    blocks_per_second = ledger_commit_df.resample("s").size()
    return blocks_per_second


if __name__ == "__main__":
    log = Log.from_json(sys.stdin)
    print("Log Dataframe")
    print(log.df)
    print("\ntxn_hash_df")
    txn_hash_df = log.txn_hash_df()
    print(txn_hash_df)
    # grouped_by = txn_hash_df.groupby("txn_hash")
    # grouped_by["timestamp"].plot(legend=True)
    # Calculate latencies

    import matplotlib.pyplot as plt
    df = txn_hash_df
    # grouped_df = df.groupby('txn_hash')
    # print(grouped_df['timestamp'].diff())
    # for key, item in grouped_df:
    #     print(key, item)
    #     print(item['timestamp'].diff())
    #     print(item['timestamp'].diff().dt.total_seconds() * 1000)
    #     print()
    #     print()
    x = df.copy()
    x["diff"] = x.groupby('txn_hash')['timestamp'].diff()
    latencies_1 = x.groupby('txn_hash').nth(1)['diff'].dt.total_seconds() * 1000
    latencies_2 = x.groupby('txn_hash').nth(2)['diff'].dt.total_seconds() * 1000
    latencies_3 = x.groupby('txn_hash').nth(3)['diff'].dt.total_seconds() * 1000
    
    # Create histograms
    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 5))
    
    ax1.hist(latencies_1, bins=20)
    ax1.set_title('Latency 1: sendRawTransaction to txpool insertion')
    ax1.set_xlabel('Latency (ms)')
    ax1.set_ylabel('Frequency')
    
    ax2.hist(latencies_2, bins=20)
    ax2.set_title('Latency 2: txpool insertion to proposal inclusion')
    ax2.set_xlabel('Latency (ms)')
    ax2.set_ylabel('Frequency')
    
    ax3.hist(latencies_3, bins=20)
    ax3.set_title('Latency 3: proposal inclusion to commitment')
    ax3.set_xlabel('Latency (ms)')
    ax3.set_ylabel('Frequency')
    
    plt.tight_layout()
    plt.show()
    
    # Print summary statistics
    print(df.groupby('txn_hash')[['latency_1', 'latency_2', 'latency_3']].describe())
