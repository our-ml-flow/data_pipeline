from task.dune_nft_trades_load_task import load_dune_nft_trade
from prefect import flow

@flow(log_prints=True)
def load_flow(engine, dune_nft_trades_df):
    load_dune_nft_trade(engine, dune_nft_trades_df)