from prefect import task
from datetime import datetime, timedelta

import pandas as pd

@task(log_prints=True)
def extract_daily_whale(engine) -> pd.DataFrame|None:
    result = None
    
    today_datetime = datetime.today() - timedelta(days=1)
    today = today_datetime.strftime('%Y-%m-%d')
    
    query= f"""
        WITH daily_basic AS (
            SELECT 
                COALESCE(buy.block_time, sell.block_time) AS date, 
                COALESCE(buy.address, sell.address) AS address, 
                COALESCE(buy.total_bought_amount, 0) AS total_bought_amount, 
                COALESCE(sell.total_sold_amount, 0) AS total_sold_amount
            FROM (
                SELECT
                    block_time,
                    buyer AS address, 
                    SUM(amount_usd) AS total_bought_amount
                FROM dune_nft_trades
                WHERE DATE(block_time) = DATE('{today}')
                GROUP BY block_time, buyer
                ) buy
            FULL OUTER JOIN (
                SELECT
                    block_time,
                    seller AS address, 
                    SUM(amount_usd) AS total_sold_amount
                FROM dune_nft_trades
                WHERE DATE(block_time) = DATE('{today}')
                GROUP BY block_time, seller
                ) sell
            ON buy.block_time = sell.block_time 
            AND buy.address = sell.address
        ),
        rank_table AS (
            SELECT
                date,
                address,
                total_bought_amount,
                total_sold_amount,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_sold_amount desc) sold_rank,
                ROW_NUMBER() OVER (PARTITION BY date ORDER BY total_bought_amount desc) bought_rank
            FROM daily_basic AS db
            WHERE db.address NOT IN (SELECT participant FROM black_list)
        ),
        rank_table_100 AS (
            SELECT 
                *
            FROM rank_table
            WHERE 
                bought_rank <= 100
                or sold_rank <= 100
            ORDER BY 1
        )
        SELECT * FROM rank_table_100;
        """    
    
    try:
        result = pd.read_sql(query, engine)
        
        print('Success extract daily whale')
        
    except Exception as e:
        print('Fail extract daily whale')
        
        raise
        
    finally:
        return result    

@task(log_prints=True)
def load_daily_whale(engine, daily_whale_df: pd.DataFrame = None):
    try:
        if daily_whale_df == None:
            raise ValueError
        
        daily_whale_df.to_sql("daily_whale_100", engine, if_exists='append', index=False)
    
    except Exception as e:
        print('Fail load daily_whale')
        
        raise
    
    else:
        print('Success load daily_whale')
