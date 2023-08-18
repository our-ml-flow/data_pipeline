import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine, get_json_block
from module.warehouse.alchemy_getnfts_daily.task import get_alchemy_json_block,load_data_table,make_dataframe,load_to_db

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import time
from datetime import datetime


@flow(name='Alchemy_get_nfts')
def alchemy_getnfts_daily():
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()  
    ALCHEMY_API_KEY = get_alchemy_json_block('alchemy-api-key')
    owner_address_df = load_data_table(engine)
    alchemy_get_nfts_df = make_dataframe(ALCHEMY_API_KEY, owner_address_df)
    load_to_db(engine, alchemy_get_nfts_df, 'test_alchemy_get_nfts')


if __name__=='__main__':

    alchemy_getnfts_daily()