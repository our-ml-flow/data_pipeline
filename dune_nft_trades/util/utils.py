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
def get_sql_engine(block_name: str) -> sqlalchemy.engine.base.Engine|None:
    engine = None
    
    try:
        database_block = SqlAlchemyConnector.load(block_name)
        
        engine = database_block.get_engine()
        
        print(f'engine: {engine}')
        
    except Exception as e:
        print(e.__doc__)
        
    finally:
        return engine

@task(log_prints=True)
def get_json_block(block_name: str, key: str) -> str|None:
    api_key = None
    
    try:    
        block = JSON.load(block_name)
        
        api_key = block.value[key]
        
    except Exception as e:
        print(e.__doc__)
        
    finally:
        return api_key
    
@task(log_prints=True)
def get_dune_api_info(engine: sqlalchemy.engine.base.Engine = None) -> pd.DataFrame|None:
    result = None
    
    try:
        if engine == None:
            raise ValueError
    
    except ValueError:
        print('No sql engine')
        
        return None
    
    else:
        query = """SELECT "address", "usage"
            FROM dune_api_info
            WHERE 2500 - "usage" >= 500
            ORDER BY 2 DESC
            LIMIT 1; 
            """
            
        try:
            dune_api_info_df = pd.read_sql(query, engine)
        
        except Exception as e:
            print('Fail get dune api info')
            print(e.__doc__)
            
        else:
            result = dune_api_info_df.iloc[0].values
        
        finally:
            return result