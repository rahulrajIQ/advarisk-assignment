import pandas as pd
import numpy as np
import datetime as dt


def extract_sales():

    df = pd.read_csv('sales_data_sample.csv')


    # Save to local with date
    df.to_csv('extracted_sales_data_'+ dt.datetime.today().strftime('%Y-%m-%d')+'.csv', index=False)


if __name__ == "__main__":
    extract_sales()

