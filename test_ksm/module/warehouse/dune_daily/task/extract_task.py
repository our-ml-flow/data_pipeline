from prefect import task

import pandas as pd
import requests


@task(name='Get dune data',
      log_prints=True)
def get_dune_data(api_key):

    if api_key != None :
        query_id = 2759265

        base_url = f"https://api.dune.com/api/v1/query/{query_id}/results?api_key={api_key}"
        
        response = requests.get(base_url)

        print("Success: Get dune response")

        return response
    
    else:
        print("Error: No Dune API KEY")



@task(name='Extract dune trades dataframe with dune data',
      log_prints=True)
def extract_dune_trades_data(response):

    if response.status_code == 200:

        print("Success: Get dune response 200")

        response_json = response.json()

        dune_trades_df = pd.DataFrame(response_json['result']['rows'])

        return dune_trades_df
    
    else:
        print("Error: Request failed")