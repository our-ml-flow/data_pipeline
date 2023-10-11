import os
import sys

repo_dir = os.path.abspath(__file__).split('/flow')[0]
print(repo_dir)
sys.path.append(f'{repo_dir}')

from module.utils import get_sql_engine, get_json_block, get_dune_api_info, log_dune_datapoint, update_dune_api_info_usage, telegram_msg_send
from module.warehouse.dune_daily.task import get_dune_nft_trade, extract_dune_trades_value, transform_drop_na, reset_columns, load_dune_nft_trade
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.blocks.system import JSON
from prefect import flow
from datetime import datetime, timedelta

import traceback

@flow(name='Dune_daily_warehouse_flow', log_prints=True)
def dune_nft_trades():
    json_block = JSON.load('telegram')
    
    bot_token, chat_id = json_block.value["mlops_bot"].values()
    
    start_time = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    
    start_msg = '\n'.join(['Dune Pipeline Start', start_time])
    
    telegram_msg_send(bot_token, chat_id, start_msg)
    
    sql_block_name = 'gcp-mlops-sql-postgres'
    
    dune_json_block_name = 'dune-api-key'
    
    try:
        sql_block = get_sql_engine(sql_block_name)
        
        engine = sql_block.get_engine()
        
        dune_api_table = get_dune_api_info(engine)
        
        dune_api_trial = 1
        for label, row in dune_api_table.iterrows():
            if dune_api_trial > 3:
                raise ValueError
            
            api_id, owner, usage = row
            
            dune_api_key = get_json_block(dune_json_block_name, api_id)
            
            response = get_dune_nft_trade(dune_api_key)
            
            if response == None:
                dune_api_trial += 1
                continue
            
            else:
                break
            
        dune_data_point = log_dune_datapoint(response)
        
        dune_df = extract_dune_trades_value(response)
        
        dune_df, before_len, after_len = transform_drop_na(dune_df)
        
        dune_df = reset_columns(dune_df)
        
        load_dune_nft_trade(engine, dune_df)
        
        after_usage = update_dune_api_info_usage(engine, api_id, usage, dune_data_point)    
    
    except Exception as e:
        error_time = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        
        error_header = e.__doc__
        
        error_body = str(e)
        
        error_trace = traceback.format_exc()
        
        error_msg = '\n'.join(['Dune Pipeline Error', error_time, '',
                            error_header, '', error_body, '', error_trace])
        
        telegram_msg_send(bot_token, chat_id, error_msg)
        
        raise 

    else:
        end_time = datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        
        end_msg = '\n'.join(['Dune Pipeline End', end_time, '',
                            f'api_id: {api_id}',
                            f'owner: {owner}',
                            f'before_usage: {usage}',
                            f'after_usage: {after_usage}',
                            f'today_data_length: tr_before - {before_len}, tr_after - {after_len}'])

        telegram_msg_send(bot_token, chat_id, end_msg)

if __name__=="__main__":
    deployment = Deployment.build_from_flow(
        flow=dune_nft_trades,
        name="LSH: Test Dune Nft Trades Flow Deployment",
        version=0.0,
        work_queue_name="dune-agent",
        schedule=(CronSchedule(cron="40 09 * * *", timezone="Asia/Seoul"))
    )
    
    deployment.apply()