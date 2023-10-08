from prefect import task
import pandas as pd
from datetime import datetime,timedelta

@task(log_prints=True)
def preprocess_collection_for_owner(df):
    current_date = datetime.today()
    target_date = current_date - timedelta(days=1)

    new_col_name = {'name':'collection_name','openSeaSlug':'opensea_slug','openSea':'opensea_floor_price',
                    'externalUrl':'external_url','twitterUsername':'twitter_username','discordUrl':'discord_url',
                    'totalBalance':'total_balance','numDistinctTokensOwned':'num_distinct_tokens_owned',
                    'isSpam':'is_spam','displayNft':'display_nft'}

    df.rename(columns=new_col_name, inplace=True)

    if 'bannerImageUrl' in df.columns:
        df.drop(columns=['bannerImageUrl'], inplace=True)
    
    df['total_balance']=pd.to_numeric(df['total_balance'], errors='coerce')
    df['num_distinct_tokens_owned']=pd.to_numeric(df['num_distinct_tokens_owned'], errors='coerce')
    df['data_created_at'] = target_date
    
    return df