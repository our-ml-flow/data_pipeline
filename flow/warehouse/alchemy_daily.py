import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine, get_json_block
from module.warehouse.alchemy.task import create_collection_for_owner,create_owners_for_contract,get_db_data

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

import time
from datetime import datetime


@flow(name='get_owner')
def create_db_owners_for_contract():
    
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    contracts=['0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D','0xBd3531dA5CF5857e7CfAA92426877b022e612cf8']
    ALCHEMY_API_KEY=get_json_block('alchemy-api-key','alchemy1')
    
    db_owners_for_contract= create_owners_for_contract(contracts,ALCHEMY_API_KEY)
    start_time=time.time()
    db_owners_for_contract.to_sql('alchemy_owners_for_contract', con=engine, if_exists='append', index=False)
    print("owners to_sql duration: {} seconds".format(time.time() - start_time))



@flow(name="get_collection")
def create_db_collection_for_owner():
    
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    owners = get_db_data('SELECT DISTINCT "owner" FROM alchemy_owners_for_contract LIMIT 200;')
    ALCHEMY_API_KEY = get_json_block('alchemy-api-key','alchemy1')

    db_collection_for_owner = create_collection_for_owner(owners,ALCHEMY_API_KEY)
    start_time=time.time()
    db_collection_for_owner.to_sql('alchemy_collection_for_owner', con=engine, if_exists='append', index=False)
    print("owners to_sql duration: {} seconds".format(time.time() - start_time))


@flow(name='Alchemy_daily_warehouse_flow', log_prints=True)
def alchemy_daily():
    create_db_owners_for_contract()
    time.sleep(2)
    create_db_collection_for_owner()

if __name__=='__main__':

    deployment=Deployment.build_from_flow(
        flow=alchemy_daily,
        name='Alchemy_daily_warehouse_flow_Deployment',
        version=1.0,
        work_queue_name='alchemy-agent',
        schedule=(CronSchedule(cron="00 11 * * *", timezone="Asia/Seoul"))
    )
    deployment.apply()
