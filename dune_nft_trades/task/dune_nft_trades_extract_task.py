from requests import get, post, ConnectionError, Response
from prefect import task, get_run_logger

import pandas as pd

@task(log_prints=True)
def get_dune_nft_trade(api_key: str = None) -> Response|None:
    response = None
    
    try:
        if api_key == None:
            raise ValueError
        
    except ValueError:
        print("No dune api key")
        
    else:
        query_id = 2759265
        
        url = f"https://api.dune.com/api/v1/query/{query_id}/results?api_key={api_key}"
        
        response = get(url)
        
        try:
            if response.status_code != 200:
                raise ConnectionError
            
        except ConnectionError:
            print("No dune response 200")
            
            response = None
        
        except Exception as e:
            print(e.__doc__)
            
            response = None
        
        else:
            print("Dune response 200")

    finally:
        return response

@task(log_prints=True)
def extract_dune_trades_value(response: Response = None) -> pd.DataFrame:
    dune_nft_trades_df = None
    
    try:
        if response == None:
            raise ValueError
        
    except ValueError:
        print("Response value error")
        
    else:
        try:
            req_json = response.json()
            
            rows = req_json['result']['rows']
            
        except Exception as e:
            print(e.__doc__)
            
        else:
            dune_nft_trades_df = pd.DataFrame(rows)
            
            print("Success parsing json")
        
    finally:    
        return dune_nft_trades_df