import pandas as pd
import numpy as np
import datetime as dt
import re



def transform_sales():

    df = pd.read_csv('extracted_sales_data_'+ dt.datetime.today().strftime('%Y-%m-%d')+'.csv')


    # Renaming cols to be more clear
    df = df.rename(columns={'ORDERNUMBER': 'order_nmber', 'QUANTITYORDERED':'quantity_ordered', 'PRICEEACH': 'price_per_item', 
                            'ORDERLINENUMBER':'order_line_no','ORDERDATE':'order_date',
        'PRODUCTLINE':'product_line','PRODUCTCODE':'product_code', 'CUSTOMERNAME': 'customer_name', 
        'ADDRESSLINE1':'address_line_1', 'ADDRESSLINE2':'address_line_2','POSTALCODE':'postal_code',
        'CONTACTLASTNAME':'contact_first_name', 'CONTACTFIRSTNAME':'contact_last_namme',
        'DEALSIZE':'deal_size'})
    

    # Changing all cols to lower case
    df.columns = map(str.lower, df.columns)

    # order_date to datetime conversion
    df.order_date = pd.to_datetime(df.order_date, format = 'mixed',dayfirst=True)

    # order_date dtype to str conversion
    df['order_date'] =df['order_date'].astype(str)

    # concating address and dropping address_line_1 and address_line_2
    df["address"] = df["address_line_1"].fillna(' ') + "  " + df["address_line_2"].fillna('')   
    df.drop(['address_line_1','address_line_2'], axis=1, inplace=True)


    df.drop_duplicates(keep='first', inplace=True)


    #Just removing . for now
    list_of_chars = ['.']
    # Create regex pattern to match all characters in list
    pattern = '[' +  ''.join(list_of_chars) +  ']'

    df.phone= df.phone.apply(lambda x: re.sub(pattern,'', x))


    dt.datetime.today().strftime('%Y-%m-%d')
    df.to_csv('transformed_sales_data_'+dt.datetime.today().strftime('%Y-%m-%d')+'.csv', index=False)


if __name__ == "__main__":
    transform_sales()

