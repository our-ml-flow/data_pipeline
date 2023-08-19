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

# 쿼리문 이용해서 블랙리스트에 포함되어있는 지갑 제거 후 오늘날짜의 전체 거래 중 금액순서로 상위 10% & 거래 금액 1천불 이상 seller
# return DataFrame -> to_sql로 데일리 고래 순위로 계속 저장, sellerr만 리스트형식:get_top_seller()['seller']로
@task(log_prints=True)
def get_top_seller():
    engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
    connection = engine.connect()
    current_date = datetime.today()
    target_date = current_date - timedelta(days=1)
    date = target_date.strftime('%Y-%m-%d')
    try:
        query = f"""
                WITH normal_trx AS (
                SELECT *
                FROM dune_nft_trades AS t
                LEFT JOIN black_list AS b ON t.seller = b.participant OR t.buyer = b.participant 
                WHERE b.participant IS NULL AND 
                t.block_time::date = '{date}'
                )
                , total_trx_count AS (
                    SELECT COUNT(*) AS total_count
                    FROM normal_trx
                )
                , selected_trx AS (
                    SELECT block_time, 
                        seller,
                        sum(amount_usd) AS amt
                    FROM normal_trx
                    GROUP BY block_time, seller
                )
                SELECT block_time, 
                    ROW_NUMBER() OVER (ORDER BY amt DESC) AS row_num,
                    seller,
                    amt
                FROM selected_trx
                CROSS JOIN total_trx_count
                WHERE amt>=1000
                ORDER BY amt DESC
                LIMIT (SELECT ROUND(total_count * 0.1) FROM total_trx_count);
                """
        result = connection.execute(text(query))
        rows=result.fetchall()
        df=pd.DataFrame(rows)
    except Exception as e:
        print("error", e)
    return df


# 쿼리문 이용해서 블랙리스트에 포함되어있는 지갑 제거 후 오늘날짜의 전체 거래 중 금액순서로 상위 10% & 거래 금액 1천불 이상 buyer
# return DataFrame -> to_sql로 데일리 고래 순위로 계속 저장, buyer만 리스트형식:get_top_buyer()['buyer']로
@task(log_prints=True)
def get_top_buyer():
    engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
    connection = engine.connect()
    current_date = datetime.today()
    target_date = current_date - timedelta(days=1)
    date = target_date.strftime('%Y-%m-%d')
    try:
        query = f"""
                WITH normal_trx AS (
                SELECT *
                FROM dune_nft_trades AS t
                LEFT JOIN black_list AS b ON t.seller = b.participant OR t.buyer = b.participant 
                WHERE b.participant IS NULL AND 
                t.block_time::date = '{date}'
                )
                , total_trx_count AS (
                    SELECT COUNT(*) AS total_count
                    FROM normal_trx
                )
                , selected_trx AS (
                    SELECT block_time, 
                        buyer,
                        sum(amount_usd) AS amt
                    FROM normal_trx
                    GROUP BY block_time, buyer
                )
                SELECT block_time,
                    ROW_NUMBER() OVER (ORDER BY amt DESC) AS row_num,
                    buyer,
                    amt
                FROM selected_trx
                CROSS JOIN total_trx_count
                WHERE amt>=1000
                ORDER BY amt DESC
                LIMIT (SELECT ROUND(total_count * 0.1) FROM total_trx_count);
                """
        result = connection.execute(text(query))
        rows=result.fetchall()
        df=pd.DataFrame(rows)
        
    except Exception as e:
        print("error", e)
    return df

