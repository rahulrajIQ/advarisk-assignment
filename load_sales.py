import pandas as pd
import numpy as np
import datetime as dt
import psycopg2
from sqlalchemy import create_engine




def create_database():
    """
    - Creates and connects to the advarisk
    - Returns the connection and cursor to advarisk
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=xyz")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    cur.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'advarisk';")
    conn.commit()
    
    # create advarisk database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS advarisk")
    cur.execute("CREATE DATABASE advarisk WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to advarisk database
    conn = psycopg2.connect("host=127.0.0.1 dbname=advarisk user=postgres password=xyz")
    cur = conn.cursor()
    
    return cur, conn


def load_sales():

    df = pd.read_csv('transformed_sales_data_'+ dt.datetime.today().strftime('%Y-%m-%d')+'.csv')


    cur, conn = create_database()

    
    cur.execute("DROP TABLE IF EXISTS sales_data")
    conn.commit()
    

    
    engine = create_engine('postgresql://postgres:xyz@localhost:5432/advarisk')
    df.to_sql('sales_data', engine,if_exists='append')

    conn.commit()
    print('Done importing {}'.format('sales_data'))

    conn.close()


    

if __name__ == "__main__":
    load_sales()

