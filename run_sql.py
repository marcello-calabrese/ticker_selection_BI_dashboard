# Script to run the ETL process

''' 
The script is used for extracting, transforming, and loading data into a mysql database.

'''

# import main libraries

import pandas as pd
import numpy as np
import yfinance as yf
import pymysql
from sqlalchemy import create_engine

### Application Layer ###

############################# extraction function ##############################################################

def extract_ticker():
    # extract the ticker data
    
    msft = yf.Ticker('MSFT')
    zion = yf.Ticker('ZION')
    ibm = yf.Ticker('IBM')
    jnj = yf.Ticker('JNJ')
    mcd = yf.Ticker('MCD')
    # create a dictionary to store the data
    dct2 = {'Company_name': [msft.info['longName'], zion.info['longName'], ibm.info['longName'], jnj.info['longName'], mcd.info['longName']],
                    'Company_ticker': [msft.info['symbol'], zion.info['symbol'], ibm.info['symbol'], jnj.info['symbol'], mcd.info['symbol']],
                    'Closed_price': [msft.info['previousClose'], zion.info['previousClose'], ibm.info['previousClose'], jnj.info['previousClose'], mcd.info['previousClose']],
                    'Company_info': [msft.info['longBusinessSummary'], zion.info['longBusinessSummary'], ibm.info['longBusinessSummary'], jnj.info['longBusinessSummary'], mcd.info['longBusinessSummary']],
                    'Company_PE': [msft.info['trailingPE'], zion.info['trailingPE'], ibm.info['trailingPE'], jnj.info['trailingPE'], mcd.info['trailingPE']],
                    'Company_cash_flow': [msft.info['operatingCashflow'], zion.info['operatingCashflow'], ibm.info['operatingCashflow'], jnj.info['operatingCashflow'], mcd.info['freeCashflow']],
                    'Company_dividend': [msft.info['dividendRate'], zion.info['dividendRate'], ibm.info['dividendRate'], jnj.info['dividendRate'], mcd.info['dividendRate']]}
        
    # create a dataframe to store the data
    df = pd.DataFrame(dct2)
    # return the dataframe
    return df

############################# transformation function #######################################################################

def transform_data():
    # create a dataframe to store the data
    df_transformed = extract_ticker()
    # round the values of the dataset to 2 decimal places
    df_transformed = df_transformed.round(2)
    return df_transformed
  

############################# loading function #######################################################################

def load_sql():
    # create a an engine to connect to the database
    db_engine = create_engine("mysql+pymysql://{user}:{pw}@localhost/{db}"
                       .format(user="root",
                               pw="Xxxxx",
                               db="yahoo_tickers"))
    
    # call the transform function
    df_toload = transform_data()
    
    # write the dataframe to the database
    
    df_toload.to_sql('ticker_selection', db_engine, if_exists='replace', index=False)
    return True
    
   
    


############################# main function #######################################################################

def main():
    # call the extract function
    #extract_ticker()
    # call the transform function
    transform_data()
    # call the load function
    load_sql()
    return True

### Run Layer ###

if __name__ == '__main__':
    main()
    print('Data has been extracted, transformed, saved in a mysql database named yahoo_tickers ')


    
    
    
