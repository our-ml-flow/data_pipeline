from prefect import task

import pandas as pd
import requests

# df의 Null값 제거
@task(name='Remove null value',
      log_prints=True)
def remove_null_value(dune_trades_df):

    dune_trades_df = dune_trades_df.dropna()

    return dune_trades_df


# df의 column 순서 재배치
@task(name='Rearrange column order',
      log_prints=True)
def rearrange_column_order(dune_trades_df):

    columns = ["block_time", "number_of_items", "amount_original", "amount_usd", "token_id", "buyer", "market", "trade_category", "trade_type", "seller", "blockchain", "currency_symbol", "nft_contract_address"]
    
    dune_trades_df = dune_trades_df[columns]

    return dune_trades_df

