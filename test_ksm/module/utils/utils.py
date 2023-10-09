from prefect.blocks.system import JSON
from prefect_sqlalchemy import SqlAlchemyConnector
from prefect import task, get_run_logger
from requests import Response
from sqlalchemy import Table, MetaData

import sqlalchemy
import pandas as pd

### 2023.10.08 기준 test용으로 만든 api key 사용해 테스트할 것

"""
Dependency:
    - prefect cloud login -k [your_api_key] --workspace [your_work_space]
    
"""

@task(name='Connect sql engine', 
      log_prints=True)
def get_sql_engine(block_name):
    database_block = None
    
    try:
        database_block = SqlAlchemyConnector.load(block_name)
        
        print(f'block: {database_block}')
        
    except Exception as e:
        print(e.__doc__)
        
    finally:
        return database_block
    

@task(name='Get dune api key',
      log_prints=True)
def get_json_block(block_name: str, key: str) -> str|None:
    api_key = None
    
    try:    
        block = JSON.load(block_name)
        
        api_key = block.value[key]
        
    except Exception as e:
        print(e.__doc__)
        
    finally:
        return api_key
    

@task(name='Get dune api information',
      log_prints=True)
def get_dune_api_info(engine):
    result = None
    
    try:
        if engine == None:
            raise ValueError
    
    except ValueError:
        print('Error: No sql engine')
        
        return None
    
    else:
        query = """
                    SELECT "address", "usage"
                    FROM dune_api_info
                    WHERE 2500 - "usage" >= 500
                    ORDER BY 2 DESC
                    LIMIT 1; 
                """
            
        try:
            dune_api_info_df = pd.read_sql(query, engine)
        
        except Exception as e:
            print('Error: Fail to get dune api info')
            print(e.__doc__)
            
        else:
            result = dune_api_info_df.iloc[0].tolist()
        
        finally:
            return result


@task(name='Get dune datapoint', 
      log_prints=True)
def log_dune_datapoint(response: Response = None) -> int|None:
    datapoint = None
    
    try:
        if response == None:
            raise ValueError
        
    except ValueError:
        print('Error: Response value error')
        
    else:
        try:
            response_json = response.json()
            
            datapoint_count = response_json['result']['metadata']['datapoint_count']
            
            datapoint = round(datapoint_count/1000)
        
        except Exception as e:
            print('Error: Fail to log dune datapoint')
            print(e.__doc__)
        
        finally:
            return datapoint


@task(name='Update dune datapoint', 
      log_prints=True)
def update_dune_api_info_usage(engine, dune_api_address, before_usage, add_usage):
    try:
        if engine == None:
            raise ValueError
        
    except ValueError:
        print('Error: No engine object')
        
    else:
        try:
            conn = engine.connect()
            
            table = Table('dune_api_info', MetaData(), autoload_with=engine)
            
            qr = table.update().where(table.c.address == dune_api_address).values(usage=int(before_usage+add_usage))

            conn.execute(qr)
            
        except Exception as e:
            print('Error: Fail to update dune api info usage')
            
            print(e.__doc__)
            
        else:
            conn.commit()
            
            print('Success: Update dune api info usage')