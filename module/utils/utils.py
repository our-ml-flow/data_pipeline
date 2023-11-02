from prefect.blocks.system import JSON
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import task, get_run_logger
from requests import Response
from sqlalchemy import Table, MetaData

import sqlalchemy
import pandas as pd
import telegram

"""
Dependency:
    - prefect cloud login -k [your_api_key] --workspace [your_work_space]
    
"""
@task(log_prints=True)
async def telegram_msg_send(bot_token, chat_id, text):
    bot = telegram.Bot(token = bot_token)
    
    await bot.sendMessage(chat_id = chat_id, text = text)
    
@task(log_prints=True)
def get_sql_engine(block_name: str) -> sqlalchemy.engine.base.Engine:
    try:
        database_block = SqlAlchemyConnector.load(block_name)
        
        print(f'block: {database_block}')
        
    except Exception as e:
        raise
        
    else:
        return database_block

@task(log_prints=True)
def get_json_block(block_name: str, key: str) -> str:
    try:    
        block = JSON.load(block_name)
        
        api_key = block.value[key]
        
    except Exception as e:
        raise
        
    else:
        return api_key
    
@task(log_prints=True)
def get_dune_api_info(engine: sqlalchemy.engine.base.Engine) -> pd.DataFrame:
    query = """SELECT "address", "owner", "usage"
        FROM dune_api_info
        WHERE 2500 - "usage" >= 500
        ORDER BY 2 DESC; 
        """
        
    try:
        dune_api_info_df = pd.read_sql(query, engine)
    
    except Exception as e:
        raise
    
    else:
        return dune_api_info_df
        
@task(log_prints=True)
def log_dune_datapoint(response: Response) -> int:
    try:
        response_json = response.json()
        
        datapoint_count = response_json['result']['metadata']['datapoint_count']
        
        datapoint = round(datapoint_count/1000)
    
    except Exception as e:
        raise
    
    finally:
        return datapoint
        
@task(log_prints=True)
def update_dune_api_info_usage(engine: sqlalchemy.engine.base.Engine, dune_api_address: str, before_usage: int, add_usage: int):
    try:
        conn = engine.connect()
        
        table = Table('dune_api_info', MetaData(), autoload_with=engine)
        
        after_usage = int(before_usage+add_usage)
        
        qr = table.update().where(table.c.address == dune_api_address).values(usage=after_usage)

        conn.execute(qr)
        
    except Exception as e:
        raise
        
    else:
        conn.commit()
        
        return after_usage
