from prefect import task
from datetime import datetime
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert

import sqlalchemy
import pandas as pd

@task(log_prints=True)
def load_wash_trade_wallet(engine: sqlalchemy.engine.base.Engine):
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    formatted_date = first_day_of_month.strftime('%Y-%m-%d')
    
    query = f"""
    WITH date_ranges AS (
        SELECT
            block_time AS start_date,
            DATE(block_time + INTERVAL '1' MONTH - INTERVAL '1' DAY) AS end_date
        FROM dune_nft_trades
        WHERE block_time = DATE('{formatted_date}')
        LIMIT 1
    ),
    paired_trades AS (
        SELECT
            dr.start_date,
            dr.end_date,
            yt.nft_contract_address,
            yt.token_id,
            LEAST(yt.buyer, yt.seller) AS participant1,
            GREATEST(yt.buyer, yt.seller) AS participant2,
            COUNT(*) AS trade_count
        FROM date_ranges dr
        JOIN dune_nft_trades yt
            ON yt.block_time BETWEEN dr.start_date AND dr.end_date
        GROUP BY dr.start_date, dr.end_date, yt.nft_contract_address, yt.token_id, participant1, participant2
    )
    SELECT * FROM paired_trades
    WHERE trade_count >= 2;;
    """
        
    try:    
        wash_trade_df = pd.read_sql(query, engine)
        
        print("Success extract about wash_trade")
        
    except Exception as e1:
        print("Fail extract about wash_trade")
        
    else:
        try:
            # test 종료 이후 테이블 교체 필요
            wash_trade_df.to_sql("wash_trade_wallet_month_test", engine, if_exists='append', index=False)
            
        except Exception as e2:
            print("Fail load to wash_trade_wallet_month")

@task(log_prints=True)
def load_black_list(engine: sqlalchemy.engine.base.Engine):
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    formatted_date = first_day_of_month.strftime('%Y-%m-%d')
    
    query = f"""
    WITH wash_trade AS (
        SELECT
            start_date,
            UNNEST(ARRAY[participant1, participant2]) AS participant
        FROM wash_trade_wallet_month_test
        WHERE start_date = DATE('{formatted_date}')
    ),
    distinct_wallet AS (
        SELECT
            participant,
            MIN(start_date) written_date
        FROM wash_trade
        GROUP BY participant
    ),
    result_t AS (
        SELECT
            participant,
            written_date
        FROM distinct_wallet
    )
    SELECT * FROM result_t;
    """
    
    try:    
        black_list_df = pd.read_sql(query, engine)
        
        print("Success extract about black_list")
        
    except Exception as e1:
        print("Fail extract about black_list")
        
    else:
        try:
            # test 종료 이후 테이블 교체 필요
            conn = engine.connect()
    
            table = Table('black_list_test', MetaData(), autoload_with=engine)
            
            stmt = insert(table).values(black_list_df.to_dict(orient="records"))
            
            on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=['participant'])
            
            conn.execute(on_conflict_stmt)
            
            conn.commit()
            
            conn.close()
            
        except Exception as e2:
            print("Fail load to black_list_test")