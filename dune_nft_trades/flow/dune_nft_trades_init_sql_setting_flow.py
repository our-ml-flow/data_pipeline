from util.utils import get_sql_engine, get_json_block, get_dune_api_info
from prefect import flow

@flow(log_prints=True)
def init_sql_setting_flow() -> tuple:
    sql_block_name = "gcp-mlops-sql-postgres"
    
    sql_block = get_sql_engine(sql_block_name)
    
    engine = sql_block.get_engine()
    
    dune_api_address, dune_api_usage = get_dune_api_info(engine)
    
    json_block_name = 'dune-api-key'
    
    dune_api_key = get_json_block(json_block_name, dune_api_address)
    
    # print(f'{dune_api_address} : {dune_api_usage}')
    
    return (sql_block, dune_api_address, dune_api_key, dune_api_usage)