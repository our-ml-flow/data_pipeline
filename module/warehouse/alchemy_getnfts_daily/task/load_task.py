from prefect import task
import sqlalchemy
import pandas as pd

@task(log_prints=True)
def load_to_db(engine:sqlalchemy.engine.base.Engine,df:pd.DataFrame,table_name:str):
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)

    except Exception as e:
        print("Fail load to DB")
        print(e.__doc__)
    else:
        print("Success load to DB")