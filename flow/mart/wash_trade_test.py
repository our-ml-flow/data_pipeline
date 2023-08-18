import os
import sys
import pandas as pd

repo_dir = os.path.abspath(__file__).split('/flow')[0]
sys.path.append(f'{repo_dir}')

from datetime import datetime
from prefect import flow
from sqlalchemy import Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from module.utils import get_sql_engine

@flow(log_prints = True)
def main():
    today = datetime.today()
    first_day_of_month = today.replace(day=1)
    formatted_date = first_day_of_month.strftime('%Y-%m-%d')

    sql_block_name = 'gcp-mlops-sql-postgres'

    data_block = get_sql_engine(sql_block_name)

    engine = data_block.get_engine()

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
            WHERE trade_count >= 2;
        """
        

    res_1 = pd.read_sql(query, engine)
    res_1.to_sql("wash_trade_wallet_month_test", engine, if_exists='append', index=False)
    
    print()
    
    query2 = f"""
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
    
    black_list_df = pd.read_sql(query2, engine)
    
    conn = engine.connect()
    
    table = Table('black_list_test', MetaData(), autoload_with=engine)
    
    stmt = insert(table).values(black_list_df.to_dict(orient="records"))
    
    on_conflict_stmt = stmt.on_conflict_do_nothing(index_elements=['participant'])
    
    conn.execute(on_conflict_stmt)
    
    conn.commit()
    
    conn.close()
    
    print()
    
if __name__ == '__main__':
    main()