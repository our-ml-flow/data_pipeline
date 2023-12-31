import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine, get_json_block
from module.warehouse.alchemy.task import create_collection_for_owner,preprocess_collection_for_owner
from module.warehouse.alchemy.task import get_top_seller_list,get_top_buyer_list

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import time
from datetime import datetime

from sqlalchemy.dialects.postgresql import JSONB



@flow(name="get_collection_seller")
def create_db_collection_for_seller(owners):
    
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    ALCHEMY_API_KEY = get_json_block('alchemy-api-key','alchemy1')

    db_collection_for_owner = create_collection_for_owner(owners,ALCHEMY_API_KEY)
    db_collection_for_owner = preprocess_collection_for_owner(db_collection_for_owner)
  
    db_collection_for_owner.to_sql('alchemy_collection_for_seller', con=engine, if_exists='append', index=False,
                                   dtype={"opensea_floor_price": JSONB, "contract": JSONB, "display_nft": JSONB, "image": JSONB})



@flow(name="get_collection_buyer")
def create_db_collection_for_buyer(owners):
    
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    ALCHEMY_API_KEY = get_json_block('alchemy-api-key','alchemy1')

    db_collection_for_owner = create_collection_for_owner(owners,ALCHEMY_API_KEY)
    db_collection_for_owner = preprocess_collection_for_owner(db_collection_for_owner)
    db_collection_for_owner.to_sql('alchemy_collection_for_buyer', con=engine, if_exists='append', index=False,
                                   dtype={"opensea_floor_price": JSONB, "contract": JSONB, "display_nft": JSONB, "image": JSONB})



@flow(name='Alchemy_daily_warehouse_flow', log_prints=True)
def alchemy_daily():
    sellers = get_top_seller_list()
    create_db_collection_for_seller(sellers)
    buyers = get_top_buyer_list()
    create_db_collection_for_buyer(buyers)


if __name__=='__main__':
   
    deployment=Deployment.build_from_flow(
        flow=alchemy_daily,
        name='Alchemy_daily_warehouse_flow_Deployment',
        version=1.3,
        work_queue_name='alchemy-agent',
        schedule=(CronSchedule(cron="00 12 * * *", timezone="Asia/Seoul"))
    )
    deployment.apply()
