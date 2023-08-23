import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from prefect import task
from datetime import timezone,datetime,timedelta
import time
import requests
import json
from sqlalchemy import text
from prefect_sqlalchemy import SqlAlchemyConnector
from typing import Union, Tuple, Optional



@task(log_prints=True)
def create_owners_for_contract(contracts:list, ALCHEMY_API_KEY:str) -> pd.DataFrame():
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

    with ThreadPoolExecutor(16) as executor:
        futures = [executor.submit(fetch_data, contract) for contract in contracts]
        
        for future, contract in zip(futures, contracts):
            owners_list, page_key = future.result()
            if page_key == None:
                data = {'owner_address':owners_list,'contract':[contract]*len(owners_list)}
                df_owner=pd.DataFrame(data)
                df_owners_for_contract = pd.concat([df_owners_for_contract,df_owner])
                print('******complete : no pagenation******')
            
            while page_key:
                pre_key = page_key
                owners_list, page_key = fetch_data(contract, page_key)
            
                data_list.extend(owners_list)
                data={'owner_address':data_list,'contract':[contract]*len(data_list)}
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
def create_collection_for_owner(owners:list,ALCHEMY_API_KEY:str) -> pd.DataFrame():
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
                data_with_info = [{'owner_address':owner, **item} for item in data]
                data_list.extend(data_with_info)
                print('******complete : no pagenation******')
                
            while page_key:
                pre_key = page_key
                data, page_key = fetch_data(owner, page_key)
                if data:
                    data_with_info = [{'owner_address':owner, **item} for item in data]
                    data_list.extend(data_with_info)
                    print('******complete : extend in pagenation******')
                if page_key == pre_key:
                    break 

    df_collection_for_owner=pd.DataFrame(data_list)
    
    return df_collection_for_owner


def get_top_seller_list():
    engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
    connection = engine.connect()
    current_date = datetime.today()
    target_date = current_date - timedelta(days=2)
    date = target_date.strftime('%Y-%m-%d')
    try:
        query = f"""
                SELECT address FROM daily_whale_100 
                WHERE sold_rank <=100 AND date>='{date}';
                """
        result = connection.execute(text(query))
        rows=result.fetchall()
        sellers=[row[0] for row in rows]
    except Exception as e:
        print("error", e)
    return sellers


def get_top_buyer_list():
    engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
    connection = engine.connect()
    current_date = datetime.today()
    target_date = current_date - timedelta(days=2)
    date = target_date.strftime('%Y-%m-%d')
    try:
        query = f"""
                SELECT address FROM daily_whale_100 
                WHERE bought_rank <=100 AND date>='{date}';
                """
        result = connection.execute(text(query))
        rows=result.fetchall()
        buyers=[row[0] for row in rows]
    except Exception as e:
        print("error", e)
    return buyers

