from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import task, get_run_logger

import sqlalchemy
import pandas as pd

@task(log_prints=True)
def load_dune_nft_trade(engine: sqlalchemy.engine.base.Engine, dune_nft_trades_df: pd.DataFrame):
    try:
        result = dune_nft_trades_df.to_sql("dune_nft_trades", engine, if_exists='append', index=False)
    
    except Exception as e:
        print("Fail load to dune_nft_trades")
        print(e.__doc__)
    
    else:
        print("Fail load to dune_nft_trades")