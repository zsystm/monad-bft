import pandas as pd


class Log:
    def __init__(self, df):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.set_index(df["timestamp"])
        self.df = df

    def ledger_commit_df(self):
        df = self.df[
            (self.df["target"] == "monad_ledger")
            & (self.df["fields"].apply(lambda x: x.get("message") == "committed block"))
        ]
        df = df.join(pd.json_normalize(df.pop("fields")))
        return df

    def txn_hash_df(self):
        df = self.df[self.df["fields"].apply(lambda x: "txn_hash" in x)].copy()
        df["txn_hash"] = df["fields"].apply(lambda x: x.pop("txn_hash"))
        return df

    @staticmethod
    def from_json(filepath_or_buffer):
        df = pd.read_json(filepath_or_buffer, lines=True)
        return Log(df)
