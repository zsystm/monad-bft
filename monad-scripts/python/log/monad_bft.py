import pandas as pd
import json


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
        df = df.join(pd.json_normalize(df.pop("fields")))
        df = df.set_index(df["timestamp"])
        df = df.drop("timestamp", axis=1)
        return df

    def consensus_state(self):
        df = self.df[
            (self.df["target"] == "monad_consensus_state")
        ]
        return df

    def create_proposal_df(self):
        cs = self.consensus_state()
        df = cs[cs["message"] == "Creating Proposal"]
        return df

    def try_vote_df(self):
        cs = self.consensus_state()
        df = cs[cs["message"] == "try vote"]
        return df

    def created_vote_df(self):
        cs = self.consensus_state()
        df = cs[cs["message"] == "created vote"]

        fields = pd.json_normalize(df["fields"])
        return fields

    def get_vote_from_df(self, df):
        x = df["vote"].apply(json.loads)

        print(x)

        y = pd.json_normalize(x)
        print(y)
        
        #x = df["vote"].iloc[0]

        #y = json.loads(x)
        #print(y["vote_info"]["round"])

        #vote = pd.json_normalize(df["vote"])
        #return vote

    @staticmethod
    def from_json(filepath_or_buffer):
        df = pd.read_json(filepath_or_buffer, lines=True)
        return BftLog(df)
