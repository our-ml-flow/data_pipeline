import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from dotenv import load_dotenv

from module.utils import get_sql_engine, get_json_block, get_dune_api_info, log_dune_datapoint, update_dune_api_info_usage
from module.warehouse.dune_daily.task import get_dune_data, extract_dune_trades_data, remove_null_value, rearrange_column_order, load_dune_trades_to_db

from prefect import flow
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule


@flow(name="KSM: Test dune nft trades",
      log_prints=True)
def dune_nft_trades_test_ksm():

    load_dotenv()
    DUNE_API_KEY = os.environ.get('api_key')

    engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
    
    # Extract
    response = get_dune_data(DUNE_API_KEY)
    dune_trades_df = extract_dune_trades_data(response)

    # Transform
    dune_trades_df = remove_null_value(dune_trades_df)
    dune_trades_df = rearrange_column_order(dune_trades_df)

    # Load
    table_name = 'dune_nft_trades_test_ksm'
    load_dune_trades_to_db(engine, dune_trades_df, table_name)



if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=dune_nft_trades_test_ksm,
        name="KSM: Test dune-nft-trades Flow Deployment",
        version=1.0,
        work_queue_name="ksm-test-dune-agent",
        schedule=(CronSchedule(cron="40 09 * * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()