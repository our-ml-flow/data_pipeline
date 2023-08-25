import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine
from module.mart.whale_daily.task import extract_daily_whale, load_daily_whale
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect import flow

@flow(name='whale_daily_flow', log_prints=True)
def wash_trade():
    sql_block_name = 'gcp-mlops-sql-postgres'
    
    block = get_sql_engine(sql_block_name)
    
    engine = block.get_engine()
    
    daily_whale_df = extract_daily_whale(engine)
    
    load_daily_whale(engine, daily_whale_df)
    
if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=wash_trade,
        name="Daily whale Flow Deployment",
        version=1.0,
        work_queue_name="whale-agent",
        schedule=(CronSchedule(cron="45 10 * * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()