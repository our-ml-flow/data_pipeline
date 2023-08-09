import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from prefect import task
from datetime import timezone,datetime
import time
import requests
import json
from sqlalchemy import text
from typing import Union, Tuple, Optional
from prefect_sqlalchemy import SqlAlchemyConnector



def get_owners(query:str) -> list:
    #query='select buyer from jul_2023_top100'

    engine = SqlAlchemyConnector.load('gcp-mlops-sql-postgres').get_engine()
    conn = engine.connect()
    result = conn.execute(text(query))
    rows=result.fetchall()
    owners=[row.buyer for row in rows] 
    return owners


def create_contract_for_owner(owners:list,ALCHEMY_API_KEY:str) -> Union[pd.DataFrame, None]:
    df_contract_for_owner=pd.DataFrame()
    data_list=[]

    def fetch_data(owner, page_key=None )-> None:
        #time.sleep(0.5)
        url = url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getContractsForOwner?owner={owner}&pageSize=100&withMetadata=true"

        if page_key:
            url += f"&pageKey={page_key}"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json().get("contracts", [])
            return data, response.json().get("page_key")
        else:
            print(f"{response}Error: Request failed for {owner}")
            return [],None
    with ThreadPoolExecutor(16) as executor:
        #time.sleep(0.5)
        futures = [executor.submit(fetch_data, owner) for owner in owners]
        
        for future, owner in zip(futures, owners):
            data, page_key = future.result()
        
            if data:
                data_with_info = [{**item} for item in data]
                data_list.extend(data_with_info)
                print('******complete : extend******')
                
            while page_key:
                pre_key = page_key
                data, page_key = fetch_data(owner, page_key)
                if data:
                    data_with_info = [{**item} for item in data]
                    data_list.extend(data_with_info)
                    print('******complete : extend in pagenation******')
                if page_key == pre_key:
                    break 
    
    df_contract_for_owner=pd.DataFrame(data_list)
    df_contract_for_owner['created_at'] = pd.Timestamp.utcnow().isoformat()

    return df_contract_for_owner

# def create_owners_for_contract():
#     ALCHEMY_API_KEY=os.getenv('ALCHEMY_API_KEY')
#     # engine = get_sql_engine('gcp-mlops-sql-postgres').get_engine()
#     # conn = engine.connect()
#     # result = conn.execute(text('select buyer from jul_2023_top100'))
#     # rows=result.fetchall()
#     # collections=[row.buyer for row in rows] 
#     # print(collections)
#     # collections=['0xfc82416000ba248684a4f7b9fbb56a17dbb72c3e', '0xe525fae3fc6fbb23af05e54ff413613a6573cff2', '0xa854772db43b52d7b456c2b5a40bd41627b316c2', '0xc935aaa23734fce35843829d2a39c3920172a0d6', '0x100e3c3d00749ac35aa002485ea45ef4b8ae34fe', '0x020ca66c30bec2c4fe3861a94e4db4a498a35872', '0x000000d20eaefdbd6cde939ab70ff0be2dab68d2', '0x29469395eaf6f95920e59f858042f0e28d98a20b', '0x1919db36ca2fa2e15f9000fd9cdc2edcf863e685', '0xa53496b67eec749ac41b4666d63228a0fb0409cf', '0xd73e0def01246b650d8a367a4b209be59c8be8ab', '0x385ce35599ae5e6f0eaf0f69841fffa9f41acdd5', '0xef0b56692f78a44cf4034b07f80204757c31bcc9', '0x77e3e957082ca648c1c5b0f3e6aec00ab1245186', '0xad091229354420fce427cf84de20884db20082d4', '0x66666f58de1bcd762a5e5c5aff9cc3c906d66666', '0xfa0e027fcb7ce300879f3729432cd505826eaabc', '0x1329d762e5e34e53a6c5e24bd15fa032482eb0f9', '0x0097b9cfe64455eed479292671a1121f502bc954', '0xe3f71798857fc2814eef24e154d7dd4e85742709', '0xa7b9c7cb5dfaf482ce2d3166b955e685e080cbbc', '0xcbb0fe555f61d23427740984325b4583a4a34c82', '0xcb415344cd0fc552ce7b48ee9375991ff5865895', '0x73d30ba3dc4ffd17c28cc2d75d12e50df98f29cf', '0xed2ab4948ba6a909a7751dec4f34f303eb8c7236', '0x2fc01d4f3c386efadd9edb5fcf4ebc9f9f4c824f', '0x72fae93d08a060a7f0a8919708c0db74ca46cbb6', '0x398abecc8702a78878b3fbe8ee154c625d4ead03', '0xc4da1707a7a16cf4ac15d75d6a49a684b23ee1e4', '0x127ecf853f7c0d59e415a28354e482fd41604b4f', '0x7df70b612040c682d1cb2e32017446e230fcd747', '0x68663fb06b6e718e7026aa1c2ddbc4e97ccd6c5c', '0x9bd03673f9f3db5037cd839e96675b047eed143f', '0xf15c93562bc3944a68e938ef75d2a3360d98ca57', '0xce841f783dc6d9df256af36b1cf6b3a52aa57cdc', '0x0fcbfc0a67985ee0803def22343ec4aeed02760e', '0x97c7d94d01bcbc41b80ef7cc8c5bebc3d11c6a20', '0x32ab8a9a13825861ddaa3872cdc4f100bc7210e7', '0x8bc110db7029197c3621bea8092ab1996d5dd7be', '0x63e0605491bda6e4c1c37cf818a45b836faf46ee', '0x95b9f6e91212014ed3224c8ccc17dcd5895a7f53', '0xade36882ad0783383ce4cc2b4c4c768875608102', '0x196deba33f0e704aa8397490a987f79bbe23a772', '0x7ace5cffbf9e1b1a4e8f491a64013b0e6f774136', '0xd47f6d5fb8dc684dfa75a0551889026738c5aa1c', '0x926bf90196781cf2161535d48af542889cac4d2e', '0x1908a3232eed9186b4a5b666075711d2db0200e5', '0x6f6878305930ecc4ec7c926d5e5081c19a87bc92', '0x0ce32775570ac566ce1a4c20dedfe854710e5412', '0x17b70f6b0dd3bf1322f972e84531767b8574e47e', '0xbbdd1b3c87c211e482cda98ea14fa8bf50022ca0', '0xcc1cb0fa339eb32e436dce1ba752bda72fc6edfe', '0x2244deba8c0e788a60b30b805ff970e082bdbc5a', '0xd14969938dbd45be356bffef350f6811d02332e1', '0xff0b9681ef40fd0992ef52fd5bd94e0fa21c0359', '0x7e35fe41789a6bc7668b8c5a44928b29ed7f41e2', '0x0c80a1162a061d1c63743fef56204c3fca45074f', '0x5f1ee29361206f1a129e808736f11598356c6031', '0x8ae57a027c63fca8070d1bf38622321de8004c67', '0x4f59ce7bb4777b536f09116b66c95a5d1ea8a8e6', '0x8ed9a20098be9e7d637e3a7f6e7cc998049362a4', '0x383f7adecd735684563af9c2a8e2f5c79808fc83', '0x8c4a0818f1ac97fb28405c8e87a609bca55b11a1', '0xffcd79396161f6d9207b67ed35f535e2189bdc28', '0x353c6c94f1596074379c35cb8d2165d0d61fff5f', '0xb06114b9997a4a16a078c4107d8eba74a869be4e', '0x5e2eabfaddf49615f84734724b77eeeebc5ebd71', '0xc45bef4db029e30fae8fa0401d760cf319c042e4', '0x71fcfba5268a5194577abf58c7996723cb50f114', '0x386c1ed28df54bd0be40d2d43f16380e62b03b34', '0x65249532663d15a76007688a8bfa1b109973ad41', '0x68c311a6896ba62583875bbf823a712cb4a27bca', '0xaf0f4479af9df756b9b2c69b463214b9a3346443', '0x49fabee6cadf2dd5bf738f9c0dfabbb2c8fd6a81', '0x0bfff40545a2250c3f11993e7b75dbbcb11e36ac', '0x77d20f43b5aeb69a533e2e8a64020491d3e3d198', '0xe512d2cde15ce490130160a8b18ae10230a73e61', '0xf532920bb32122d6475ad4cc7634dc3a69631902', '0x4986a3b6243be6994faf07ebafd284cdf0b39184', '0x11da290e2ed097cf7614c5ce17b437afe765489b', '0x4622295a07ad10b16809ce06b71acbec01e9db49', '0x888c5684b59c8f42128f3006d5edd404c73ec922', '0x174ed45bdd356c55fcb5336e06f3df059873cb41', '0x45133db3eadd8718f5c3a868ec2f7148e460dcfd', '0x13422774d8416a316390f5aa2e713c5742592aaf', '0x5eeeb64d0e697a60e6dacd7ad9a16a6bdd5560e2', '0x8b5abf01b87f87fb8e0ffc60d32ed7dd29b1f06b', '0x4d08a1ec94ca749be19a9209541ad4590375421d', '0x88fe1481e23916df6d5680f91ae7ff096f5d2637', '0xfd61cef0879dafb9ebfabe59b8e25d221821c722', '0x47a9f89b1587db59a9aa53a93e9eae5915e8b36f', '0xee0ad479d7815003d6b389cd849027cfc2cb6c72', '0x6a01af210dce01f44b5067fc688641384a8bce5b', '0x47d4da26ecb6bfa2d76b2b92794ffb67fb6f9165', '0x1fee3385b22d69e93209db2042be58fcac57b59b', '0x805076bb8b11466ae76cfd2d4b67bcb69f449070', '0x242559cceece552cc790bd3f2f9d890921ed6125', '0xde9b4206b1499e56e4417f8edb7be4586fed30ba', '0xda3bb6f56e35ae6c62835a659867d6a370f02e0b', '0x75f78eb0540b7ad7d1567e258d0a34b3fa071403']

#     df_owners_for_contract=pd.DataFrame()
#     data_list=[]

#     def fetch_data(collection, page_key=None):
#         #time.sleep(0.5)
        
#         url = f"https://eth-mainnet.g.alchemy.com/nft/v3/{ALCHEMY_API_KEY}/getOwnersForContract?contractAddress={collection}&withTokenBalances=true"

#         if page_key:
#             url += f"&pageKey={page_key}"
#         headers = {"accept": "application/json"}
#         response = requests.get(url, headers=headers)

#         if response.status_code == 200:
#             data = response.json().get("owners", [])
#             print(data)
#             return data, response.json().get("page_key")
#         else:
#             print(response)
#             print(f"Error: Request failed for {collection}")
#             return [],None


#     with ThreadPoolExecutor(2) as executor:
#         #time.sleep(0.5)
#         futures = [executor.submit(fetch_data, collection) for collection in collections]
        
#         for future, collection in zip(futures, collections):
#             data, page_key = future.result()
        
#             if data:
#                 data_with_info = [{**item} for item in data]
#                 data_list.extend(data_with_info)
#                 print(data_list)
#                 print('******complete : extend******')
                
#             while page_key:
#                 pre_key = page_key
#                 data, page_key = fetch_data(collection, page_key)
#                 if data:
#                     data_with_info = [{**item} for item in data]
#                     data_list.extend(data_with_info)
#                     print('******complete : extend in pagenation******')
#                 if page_key == pre_key:
#                     break 
#     df_owners_for_contract=pd.DataFrame(data_list)
#     df_owners_for_contract['created_at'] = pd.Timestamp.utcnow().isoformat()

#     return df_owners_for_contract

# # 전처리 함수...


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


#summarize_nft_attributes().to_csv('test.csv',index=False)