import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from prefect import task
from datetime import timezone,datetime
import time
import requests
import json
from sqlalchemy import text
from typing import Union, Tuple, Optional


@task(log_prints=True)
def get_owners(query:str) -> list:
    #query='select buyer from jul_2023_top100'

    engine = SqlAlchemyConnector.load('gcp-mlops-sql-postgres').get_engine()
    conn = engine.connect()
    result = conn.execute(text(query))
    rows=result.fetchall()
    owners=[row.buyer for row in rows] 
    return owners

@task(log_prints=True)
def create_owners_for_contract(contracts:list, ALCHEMY_API_KEY:str):
    #getOwnersForCollection	CU:600	Throughput CU:20
    #Free 330cu. 16request/sec
    data_list=[]
    df_owners_for_contract = pd.DataFrame()

    def fetch_data(contract, page_key=None):
        #time.sleep(0.5)
       
        url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getOwnersForContract?contractAddress={contract}&withTokenBalances=false"

        if page_key:
            url += f"&pageKey={page_key}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)

        try:
            owners_list = response.json().get("owners")
            page_key = response.json().get("page_key")
            return owners_list, page_key
        
        except Exception as e:
            print(e)
            print(f"Error: Request failed for {contract}")

    with ThreadPoolExecutor(8) as executor:
        futures = [executor.submit(fetch_data, contract) for contract in contracts]
        
        for future, contract in zip(futures, contracts):
            owners_list, page_key = future.result()
            if page_key == None:
                data = {'owner':owners_list,'contract':[contract]*len(owners_list)}
                df_owner=pd.DataFrame(data)
                df_owners_for_contract = pd.concat([df_owners_for_contract,df_owner])
                print('******complete : no pagenation******')
            
            while page_key:
                pre_key = page_key
                owners_list, page_key = fetch_data(contract, page_key)
            
                data_list.extend(owners_list)
                data={'owner':data_list,'contract':[contract]*len(data_list)}
                df_owner=pd.DataFrame(data)
                df_owners_for_contract = pd.concat([df_owners_for_contract,df_owner])
                print('******complete : in pagenation******')
                if page_key == pre_key:
                    break
    df_owners_for_contract['data_created_at'] = pd.Timestamp.utcnow().isoformat()
    df_owners_for_contract = df_owners_for_contract.reset_index(drop=True)
    return df_owners_for_contract


@task(log_prints=True)
def make_request_with_backoff(url, max_retries=10, max_backoff=64):
    retries = 0
    n = 0
    
    while retries < max_retries:
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for non-200 responses
            
            # If request is successful, return the response
            return response
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            
            # Calculate backoff time
            random_milliseconds = random.randint(0, 1000)
            backoff_time = min((2 ** n) + random_milliseconds, max_backoff)
            
            print(f"Retrying in {backoff_time} seconds...")
            time.sleep(backoff_time)
            
            # Increment retries and n
            retries += 1
            n += 1


@task(log_prints=True)
def create_collection_for_owner(owners:list,ALCHEMY_API_KEY:str):
    #getContractsForOwner	CU: 350	ThroughputCU 100
    #Free 330. 3 request/sec
    df_collection_for_owner=pd.DataFrame()
    data_list=[]

    def fetch_data(owner, page_key=None )-> None:
        url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getCollectionsForOwner?owner={owner}&pageSize=100&withMetadata=true"
        
        if page_key:
            url += f"&pageKey={page_key}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)

        try:
            data = response.json().get("collections", [])
            return data, response.json().get("page_key")
        except Exception as e:
            print(f"{e}, Error: Request failed for {owner}")
            
    with ThreadPoolExecutor(16) as executor:
        futures = [executor.submit(fetch_data, owner) for owner in owners]
        
        for future, owner in zip(futures, owners):
            data, page_key = future.result()
        
            if data:
                data_with_info = [{'owner':owner, **item} for item in data]
                data_list.extend(data_with_info)
                print('******complete : no pagenation******')
                
            while page_key:
                pre_key = page_key
                data, page_key = fetch_data(owner, page_key)
                if data:
                    data_with_info = [{'owner':owner, **item} for item in data]
                    data_list.extend(data_with_info)
                    print('******complete : extend in pagenation******')
                if page_key == pre_key:
                    break 
    
    df_collection_for_owner=pd.DataFrame(data_list).astype(str)
    df_collection_for_owner['data_created_at'] = pd.Timestamp.utcnow().isoformat()

    return df_collection_for_owner


#@task
# def summarize_nft_attributes():
#     ALCHEMY_API_KEY=os.getenv('ALCHEMY_API_KEY')
#     df_slug=pd.read_csv('Pipeline_A/module/util/slug_table.csv',index_col=False)
#     #collections=df_slug['contract_address']
#     collections=['0x0e9d6552b85be180d941f1ca73ae3e318d2d4f1f','0x837704ec8dfec198789baf061d6e93b0e1555da6','0x86ba9ec85eebe10a9b01af64e89f9d76d25cea18']
#     #print(df_slug)
#     df_summarize_nft_attributes=pd.DataFrame()
    
#     data_list=[]

#     def fetch_data(collection):
#         # time.sleep(0.2)
#         url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/summarizeNFTAttributes?contractAddress={collection}"
#         headers = {"accept": "application/json"}
#         response = requests.get(url, headers=headers)
#         data = response.json()

#         if response.status_code == 200:
#             # #print(data)
#             return data
#         else:
#             print(f"{response} error:{collection} {data['error']['message']}")
#             #print(f"error:{collection}, {response.json()}")
#             return 

#     with ThreadPoolExecutor(16) as executor:
#         futures = [executor.submit(fetch_data, collection) for collection in collections]
        
#         for index, future in enumerate(futures):
#             data = future.result()
#             print(data)
#             print(type(data))
#             data_with_info = [{**item} for item in data]
#             data_list.extend(data_with_info)
#             #print(data_list)
#             print(f'******complete : {index} task******')
            

#     df_summarize_nft_attributes=pd.DataFrame(data_list)
#     df_summarize_nft_attributes['created_at'] = pd.Timestamp.utcnow().isoformat()

#     return df_summarize_nft_attributes
