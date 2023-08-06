from util.utils import log_dune_datapoint, update_dune_api_info_usage
from task.dune_nft_trades_extract_task import get_dune_nft_trade, extract_dune_trades_value
from prefect import flow

@flow(log_prints=True)
def extract_flow(api_key) -> tuple:
    response = get_dune_nft_trade(api_key)
    
    dune_nft_trades_df = extract_dune_trades_value(response)
    
    return (response, dune_nft_trades_df)

@flow(log_prints=True)
def update_flow(engine, response, dune_api_address, before_usage):
    datapoint = log_dune_datapoint(response)
    
    update_dune_api_info_usage(engine, dune_api_address, before_usage, datapoint)