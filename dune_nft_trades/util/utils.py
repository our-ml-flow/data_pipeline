from prefect.blocks.system import JSON
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import task, get_run_logger

import sqlalchemy
import pandas as pd

"""
Dependency:
    - prefect cloud login -k [your_api_key] --workspace [your_work_space]
    
"""
@task(log_prints=True)
def get_sql_engine(block_name: str) -> sqlalchemy.engine.base.Engine:
    try:
        database_block = SqlAlchemyConnector.load(block_name)
        engine = database_block.get_engine()
        print(f'engine: {engine}')
    except Exception as e:
        print(e.__doc__)
        engine = None
    finally:
        return engine

@task(log_prints=True)
def get_json_block(block_name: str, address: str) -> str:
    try:    
        block = JSON.load(block_name)
        api_key = block.value[address]
    except Exception as e:
        print(e.__doc__)
        api_key = None
    finally:
        return api_key