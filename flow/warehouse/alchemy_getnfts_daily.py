import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine, get_json_block
from module.warehouse.alchemy_getnfts_daily.task import get_alchemy_json_block,load_data_table,extract_nfts,make_dataframe,load_to_db

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import time
from datetime import datetime


@flow(name='Alchemy_get_nfts')
def alchemy_getnfts_daily():
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()  
    ALCHEMY_API_KEY = get_alchemy_json_block('alchemy-api-key')
    owner_addresses = load_to_db(engine)
    alchemy_get_nfts_df = make_dataframe(engine, ALCHEMY_API_KEY, owner_addresses)


if __name__=='__main__':

    deployment=Deployment.build_from_flow(
        flow=alchemy_getnfts_daily,
        name='Alchemy_getnfts_daily_warehouse_flow_Deployment',
        version=1.0,
        work_queue_name='alchemy-getnft-agent',
        schedule=(CronSchedule(cron="30 11 * * *", timezone="Asia/Seoul"))
    )
    deployment.apply()