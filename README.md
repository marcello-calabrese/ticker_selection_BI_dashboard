# Data Engineering Project: 4 shares of a stock data extraction, upload on MySql used to be in a BI project

## Stock Selected and Data Extracted

The API used will be the yfinance API.
Stocks selected are: MSFT, ZION, IBM, JNJ, MCD. The data is extracted from Yahoo Finance.
Type of data extracted are:

- Company Name
- Company Ticker
- Closed Price
- Company Info
- Company P/E Ratio
- Company Cash Flow
- Company Dividend

## Data Transformation and Loading on MySql

The data is transformed into a format that is easier to be used in the BI project.
The format will be a pandas dataframe that will be created and transformed from the data extracted.
The dataframe will be used to be uploaded on MySql.

## Data Analysis

The data analysis will be done using PowerBI connected to MySQL.
