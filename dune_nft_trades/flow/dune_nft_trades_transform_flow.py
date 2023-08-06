from task.dune_nft_trades_transform_task import transform_drop_na, reset_columns
from prefect import flow

import pandas as pd

@flow(log_prints=True)
def transform_flow(dune_nft_trades_df) -> pd.DataFrame:
    dune_nft_trades_df = transform_drop_na(dune_nft_trades_df)
    
    dune_nft_trades_df = reset_columns(dune_nft_trades_df)
    
    return dune_nft_trades_df