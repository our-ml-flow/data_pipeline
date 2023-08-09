from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
import os,sys
sys.path.append(os.getcwd())
from module.utils.utils import get_sql_engine
from module.warehouse.alchemy.task.extract_task import create_nfts_for_contract, create_owners_for_contract
import pandas as pd
import time
from datetime import datetime

@flow
def crate_db_nfts_for_contract():
    db_nfts_for_contract= create_nfts_for_contract().astype(str)
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    start_time=time.time()
    db_nfts_for_contract.to_sql('nfts_for_contract_frdb', con=engine, if_exists='append', index=False)
    print("nfts to_sql duration: {} seconds".format(time.time() - start_time))
    

@flow
def create_db_owners_for_contract():
    db_owners_for_contract= create_owners_for_contract().astype(str)
    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    start_time=time.time()
    db_owners_for_contract.to_sql('owners_for_contract', con=engine, if_exists='append', index=False)
    print("owners to_sql duration: {} seconds".format(time.time() - start_time))
    

    
if __name__=='__main__':

    deployment=Deployment.build_from_flow(
        flow=crate_db_nfts_for_contract,
        name='db_nfts_for_contract',
        version=1,
        work_queue_name='q',
        schedule=(CronSchedule(cron="0 0 * * *", timezone="UTC"))
    )
    deployment.apply()


    deployment=Deployment.build_from_flow(
        flow=create_db_owners_for_contract,
        name='db_owners_for_contract',
        version=1,
        work_queue_name='q',
        schedule=(CronSchedule(cron="0 0 * * *", timezone="UTC"))
    )
    deployment.apply()