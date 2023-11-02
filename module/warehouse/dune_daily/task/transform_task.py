from prefect import task
from datetime import datetime, timedelta

import pandas as pd

@task(log_prints=True)
def transform_drop_na(dune_nft_trades_df: pd.DataFrame) -> pd.DataFrame:
    before_df_length = len(dune_nft_trades_df)
    
    dune_nft_trades_df.dropna(inplace=True)
    
    dune_nft_trades_df.sort_values('block_time')
    
    dune_nft_trades_df.reset_index(inplace=True)
    
    after_df_length = len(dune_nft_trades_df)
    
    return dune_nft_trades_df, before_df_length, after_df_length

@task(log_prints=True)
def reset_columns(dune_nft_trades_df: pd.DataFrame) -> pd.DataFrame:
    columns = ["block_time", "number_of_items", "amount_original", "amount_usd", "token_id", "buyer", "market", "trade_category", "trade_type", "seller", "blockchain", "currency_symbol", "nft_contract_address"]
    
    dune_nft_trades_df = dune_nft_trades_df[columns]
    
    return dune_nft_trades_df