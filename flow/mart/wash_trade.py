import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine
from module.mart.wash_trade_monthly.task import load_wash_trade_wallet, load_black_list
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect import flow

@flow(name='wash_trade_monthly_mart_flow', log_prints=True)
def wash_trade():
    sql_block_name = 'gcp-mlops-sql-postgres'
    
    block = get_sql_engine(sql_block_name)
    
    engine = block.get_engine()
    
    load_wash_trade_wallet(engine)
    
    load_black_list(engine)
    
if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=wash_trade,
        name="Wash trade Flow Deployment",
        version=0.0,
        work_queue_name="dune-agent",
        schedule=(CronSchedule(cron="0 11 1 * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()