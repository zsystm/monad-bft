import sys
from log.monad_bft import BftLog

if __name__ == "__main__":
    log = BftLog.from_json(sys.stdin)

    #df = log.create_proposal_df()
    #print(df.head(23))

    df = log.created_vote_df()
    #print(df.head(10))
    df = log.get_vote_from_df(df)
    #print(df.head(10))
