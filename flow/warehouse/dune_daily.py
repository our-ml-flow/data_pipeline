from module.utils.utils import get_sql_engine, get_json_block, get_dune_api_info, log_dune_datapoint, update_dune_api_info_usage
from module.warehouse.dune_daily.task.extract_task import get_dune_nft_trade, extract_dune_trades_value
from module.warehouse.dune_daily.task.transform_task import transform_drop_na, reset_columns
from module.warehouse.dune_daily.task.load_task import load_dune_nft_trade

from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect import flow
from datetime import datetime, timedelta

@flow(name='Initialize setting_flow', log_prints=True)
def init_sql_setting(sql_block_name: str, json_block_name: str) -> tuple:
    database_block = get_sql_engine(sql_block_name)
    
    engine = database_block.get_engine()
    
    api_address, api_usage = get_dune_api_info(engine)
    
    api_key = get_json_block(json_block_name, api_address)
    
    return (engine, api_address, api_usage, api_key)

@flow(name='ETL_flow', log_prints=True)
def dune_etl_flow(init_info: tuple) -> tuple|None:
    engine, api_address, api_usage, api_key = init_info
    
    response = get_dune_nft_trade(api_key)
    
    datapoint = log_dune_datapoint(response)
    
    if datapoint == None:
        extract_date = datetime.strftime(datetime.today() - timedelta(days=1), '%Y.%m.%d')
        
        print(f'{extract_date} required datapoint is more than {api_usage}')
        
        print('Fail ETL_flow')
        
        return None
    
    else:
        print(f'{extract_date} required datapoint is {datapoint}')
        
        print(f'Before usage: {api_usage}, After usage: {api_usage+datapoint}')
    
    dune_df = extract_dune_trades_value(response)
    
    dune_df = transform_drop_na(dune_df)
    
    dune_df = reset_columns(dune_df)
    
    load_dune_nft_trade(engine, dune_df)

    update_dune_api_info_usage(engine, api_address, api_usage, datapoint)
    
@flow(name='Main_flow', log_prints=True)
def dune_nft_trades():
    sql_block_name = 'gcp-mlops-sql-postgres'
    
    json_block_name = 'dune-api-key'
    
    init_info = init_sql_setting(sql_block_name, json_block_name)
    
    dune_etl_flow(init_info)

if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=dune_nft_trades,
        name="Dune nft trades Flow Deployment",
        version=1.0,
        work_queue_name="dune-agent",
        schedule=(CronSchedule(cron="30 10 * * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()