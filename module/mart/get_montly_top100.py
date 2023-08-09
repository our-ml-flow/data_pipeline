from prefect_sqlalchemy import SqlAlchemyConnector
from sqlalchemy import text
import pandas as pd

def montly_top100(month:str,start, end,limit:int):
    engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
    connection = engine.connect()
    df_jun_top100 = pd.DataFrame()

    try:
        result = connection.execute(text(f"WITH {month} AS( SELECT * FROM dune_nft_trades WHERE block_time BETWEEN '{start}' AND '{end}') SELECT DISTINCT buyer, sum(amount_usd) amt FROM {month} GROUP BY buyer ORDER BY amt DESC LIMIT {limit};"))
        rows=result.fetchall()
        df_jun_top100['buyer'] = [row.buyer for row in rows]
        df_jun_top100['amt'] = [row.amt for row in rows]

        print(df_jun_top100,len(df_jun_top100))
    except Exception as e:
        print("error", e)
    return df_jun_top100


# df_jun_top100=montly_top100('jun_2023_top100','2023-06-01', '2023-06-30',100)
# engine=SqlAlchemyConnector.load("gcp-mlops-sql-postgres").get_engine()
# df_jun_top100.to_sql('jun_2023_top100',engine, if_exists='replace', index=False)