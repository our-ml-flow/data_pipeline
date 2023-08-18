from prefect_sqlalchemy import SqlAlchemyConnector
from prefect.blocks.system import JSON
from prefect import task
from sqlalchemy import text

import concurrent.futures
import time
import pandas as pd
import os
import requests
import json
import datetime
import sqlalchemy


@task(log_prints=True)
def get_alchemy_json_block(block_name: str) -> str|None:
    ALCHEMY_API_KEY = None
    
    try:    
        block = JSON.load(block_name)   
        ALCHEMY_API_KEY = block.value['alchemy2']

    except Exception as e:
        print(e.__doc__)
        
    finally:
        return ALCHEMY_API_KEY


@task(log_prints=True)
def load_data_table(engine):   

    #query = "SELECT DISTINCT owner_address FROM alchemy_owners_for_contract LIMIT 200;"
    query = " SELECT DISTINCT owner_address FROM alchemy_collection_for_buyer UNION SELECT DISTINCT owner_address FROM alchemy_collection_for_seller"
    owner_address_df = pd.read_sql(query, engine)

    return owner_address_df


def extract_nfts(api_variable):

    ALCHEMY_API_KEY, owner_address = api_variable

    url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{ALCHEMY_API_KEY}/getNFTs?owner={owner_address}&withMetadata=true&orderBy=transferTime&pageSize=100"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        page_key = data.get("pageKey")
        total_count = data.get("totalCount")  # 총 NFT 개수
        block_hash = data.get("blockHash")    # 블록 해시
        owned_nfts = data.get("ownedNfts", [])
        ################################################################
        print(f"{owner_address} -> response.status_code == 200")
        ################################################################
        data_created_at = datetime.datetime.utcnow()  # 생성 시간
        nft_list = []

        for nft in owned_nfts:   
            nft_info = {
                "data_created_at": data_created_at,
                "owner_address": owner_address,
                "contract_address": nft.get("contract", {}).get("address", "None"),
                "token_id": nft.get("id", {}).get("tokenId", "None"),
                "tokenType": nft.get("id", {}).get("tokenMetadata", "None").get("tokenType", "None"),
                "balance": nft.get("balance", "None"),
                "acquired_at": nft.get("acquiredAt", {}).get("blockTimestamp", "None"),
                "block_number": nft.get("acquiredAt", {}).get("blockNumber", "None"),
                "title": nft.get("title", "None"),
                "description": nft.get("description", "None"),
                "token_uri": nft.get("tokenUri", {}).get("gateway", "None"),
                "media": nft.get("media", "None"),
                "metadata_name": nft.get("metadata", "None"),
                "time_last_updated": nft.get("timeLastUpdated", "None"),
                "contract_metadata": nft.get("contractMetadata", "None"),
                "total_count": total_count,
                "block_hash": block_hash
            }
            nft_list.append(nft_info)
        
        # pageKey가 있는 경우 다음 페이지 요청
        while page_key:
            ################################################################
            print(f"page_key: {page_key}")
            ################################################################
            next_url = f"{url}&pageKey={page_key}"
            next_response = requests.get(next_url)
            if next_response.status_code == 200:
                next_data = next_response.json().get("data", {})
                page_key = next_data.get("pageKey")
                owned_nfts = next_data.get("ownedNfts", [])
                
                for nft in owned_nfts:
                    nft_info = {
                        "data_created_at": data_created_at,
                        "owner_address": owner_address,
                        "contract_address": nft.get("contract", {}).get("address", "None"),
                        "token_id": nft.get("id", {}).get("tokenId", "None"),
                        "token_type": nft.get("id", {}).get("tokenMetadata", "None").get("tokenType", "None"),
                        "balance": nft.get("balance", "None"),
                        "acquired_at": nft.get("acquiredAt", {}).get("blockTimestamp", "None"),
                        "block_number": nft.get("acquiredAt", {}).get("blockNumber", "None"),
                        "title": nft.get("title", "None"),
                        "description": nft.get("description", "None"),
                        "token_uri": nft.get("tokenUri", {}).get("gateway", "None"),
                        "media": nft.get("media", "None"),
                        "metadata_name": nft.get("metadata", "None"),
                        "time_last_updated": nft.get("timeLastUpdated", "None"),
                        "contract_metadata": nft.get("contractMetadata", "None"),
                        "total_count": total_count,
                        "block_hash": block_hash
                    }
                    nft_list.append(nft_info)
                    ################################################################
                    print(f"******{owner_address} -> nft_info append******")
                    ################################################################
                
        return nft_list
    
    else:
        print(f"Error: Request failed for {owner_address}")
        return []


@task(log_prints=True)
def make_dataframe(ALCHEMY_API_KEY, owner_address_df):

    owner_addresses = owner_address_df['owner_address'].values.tolist()

    api_variables = zip([ALCHEMY_API_KEY]*len(owner_addresses), owner_addresses)

    nft_list = []
    with concurrent.futures.ThreadPoolExecutor(4) as executor:
        results = executor.map(extract_nfts, api_variables)
        for result in results:
            nft_list.extend(result)
            print("#########################")
            print("nft_list extend")
            print("#########################")
    # print(nft_list)

    # 데이터프레임 생성
    # df = pd.DataFrame.from_dict(nft_list)

    # 데이터프레임 생성
    alchemy_get_nfts_df = pd.DataFrame.from_dict(nft_list)
    print(alchemy_get_nfts_df)

    # JSON 데이터가 들어가는 컬럼을 문자열로 변환
    json_columns = ['data_created_at', 'contract_address', 'token_id', 'tokenType',
                    'balance', 'acquired_at', 'block_number', 'title', 'description',
                    'token_uri', 'media', 'metadata_name', 'time_last_updated',
                    'contract_metadata', 'total_count', 'block_hash', 'owner_address']
    for col in json_columns:
        alchemy_get_nfts_df[col] = alchemy_get_nfts_df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    return alchemy_get_nfts_df