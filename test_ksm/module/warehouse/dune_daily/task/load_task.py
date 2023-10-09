from prefect import task

import pandas as pd
import requests

@task(log_prints=True)
def load_dune_trades_to_db(engine, dune_trades_df, table_name):
    try:
        dune_trades_df.to_sql(table_name, engine, if_exists='append', index=False)
        
    except Exception as e:
        print("Error: Fail load to DB")
        print(e.__doc__)
        raise

    else:
        print("Success: Load to DB")