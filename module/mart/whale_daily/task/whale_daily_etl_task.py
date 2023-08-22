from prefect import task
from datetime import datetime, timedelta

import pandas as pd

@task(log_prints=True)
def extract_daily_whale(engine) -> pd.DataFrame|None:
    result = None
    
    today_datetime = datetime.today() - timedelta(days=1)
    today = today_datetime.strftime('%Y-%m-%d')
    
    query= f"""
        WITH daily_basic as (
            SELECT 
                COALESCE(buy.block_time, sell.block_time) AS date, 
                COALESCE(buy.address, sell.address) AS address, 
                COALESCE(buy.total_bought_amount, 0) AS total_bought_amount, 
                COALESCE(sell.total_sold_amount, 0) AS total_sold_amount
            FROM (
                select
                    block_time,
                    buyer AS address, 
                    SUM(amount_usd) AS total_bought_amount
                FROM dune_nft_trades
                WHERE DATE(block_time) = DATE('f{today}')
                GROUP BY block_time, buyer
                ) buy
            FULL OUTER JOIN (
                select
                    block_time,
                    seller AS address, 
                    SUM(amount_usd) AS total_sold_amount
                FROM dune_nft_trades
                WHERE DATE(block_time) = DATE('f{today}')
                GROUP BY block_time, seller
                ) sell
            ON buy.block_time = sell.block_time 
            AND buy.address = sell.address
        ),
        rank_table as (
            SELECT
                date,
                address,
                total_bought_amount,
                total_sold_amount,
                ROW_NUMBER() over (partition by date order by total_sold_amount desc) sold_rank,
                ROW_NUMBER() over (partition by date order by total_bought_amount desc) bought_rank
            FROM daily_basic as db
            WHERE db.address not in (SELECT participant FROM black_list)
        ),
        rank_table_100 as (
            select 
                *
            from rank_table
            where 
                bought_rank <= 100
                or sold_rank <= 100
            order by 1
        )
        select * from rank_table_100;
        """    
    
    try:
        result = pd.read_sql(query, engine)
        
        print('Success extract daily whale')
        
    except:
        print('Fail extract daily whale')
        
    finally:
        return result    
