from flow.dune_nft_trades_init_sql_setting_flow import init_sql_setting_flow
from flow.dune_nft_trades_extract_update_flow import extract_flow, update_flow
from flow.dune_nft_trades_transform_flow import transform_flow
from flow.dune_nft_trades_load_flow import load_flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect import flow

@flow()
def dune_nft_trades():
    sql_block, dune_api_address, dune_api_key, dune_api_usage = init_sql_setting_flow()
    
    engine = sql_block.get_engine()
    
    print(dune_api_address, dune_api_key, dune_api_usage)
    
    response, dune_nft_trades_df = extract_flow(dune_api_key)
    
    update_flow(engine, response, dune_api_address, dune_api_usage)
    
    dune_nft_trades_df = transform_flow(dune_nft_trades_df)
    
    load_flow(engine, dune_nft_trades_df)

if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=dune_nft_trades,
        name="Dune nft trades Flow Deployment",
        version=1,
        work_queue_name="dune-agent",
        schedule=(CronSchedule(cron="30 10 * * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()