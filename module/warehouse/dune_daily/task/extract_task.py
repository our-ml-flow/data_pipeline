from requests import get, ConnectionError, Response
from prefect import task

import pandas as pd

@task(log_prints=True)
def get_dune_nft_trade(api_key: str) -> Response|None:
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
def extract_dune_trades_value(response: Response) -> pd.DataFrame:
    try:
        req_json = response.json()
        
        rows = req_json["result"]["rows"]
        
    except Exception as e:
        print(e.__doc__)
        
        raise
        
    else:
        dune_nft_trades_df = pd.DataFrame(rows)
        
        return dune_nft_trades_df    