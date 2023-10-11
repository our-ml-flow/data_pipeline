from prefect import task

import sqlalchemy
import pandas as pd

@task(log_prints=True)
def load_dune_nft_trade(engine: sqlalchemy.engine.base.Engine, dune_nft_trades_df: pd.DataFrame):
    try:
        result = dune_nft_trades_df.to_sql("dune_nft_trades_test_lsh", engine, if_exists='append', index=False)
    
    except Exception as e:
        raise
    
    else:
        print("Success load to dune_nft_trades")