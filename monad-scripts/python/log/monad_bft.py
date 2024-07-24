import pandas as pd


class BftLog:
    def __init__(self, df):
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["message"] = df["fields"].apply(lambda x: x.pop("message"))
        self.df = df

    def ledger_commit_df(self):
        df = self.df[
            (self.df["target"] == "monad_ledger")
            & (self.df["message"] == "committed block")
        ]
        df = pd.concat([df.reset_index(drop=True), pd.json_normalize(df.pop("fields"))], axis=1)
        df = df.set_index(df["timestamp"])
        df = df.drop("timestamp", axis=1)
        return df

    @staticmethod
    def from_json(filepath_or_buffer):
        df = pd.read_json(filepath_or_buffer, lines=True)
        return BftLog(df)
