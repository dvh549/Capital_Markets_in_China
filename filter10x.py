import pandas as pd
# General / Common libs
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()
import pyfolio as pf
import csv 

def calculate_X(ticker):
    try:
        ticker_data = pdr.get_data_yahoo(ticker, start="2009-01-01", end="2019-12-31")
        ticker_close =ticker_data["Close"]
        ticker__adj_close = ticker_data["Adj Close"]
        # if pd.to_datetime(ticker_close.keys()[0])==pd.to_datetime("2009-01-02"):
        return round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
    
    except:
        return 0
def calculate_X_after(ticker):
    try:
        ticker_data = pdr.get_data_yahoo(ticker, start="2020-01-01", end="2022-06-30")
        ticker_close =ticker_data["Close"]
        ticker__adj_close = ticker_data["Adj Close"]
        # if pd.to_datetime(ticker_close.keys()[0])==pd.to_datetime("2020-01-02"):
        return round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
        # else:
        #     return 0
        # return round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
    except:
        print("here")
        return 0

def get_ticker_stats(ticker, start, end):
    ticker_adj_close = pdr.get_data_yahoo(ticker, start=start, end=end)["Adj Close"]
    ticker_return_ts = ticker_adj_close.pct_change().dropna()
    return pf.show_perf_stats(ticker_return_ts).Backtest.to_dict()

# print(get_ticker_stats("AAPL", "2020-01-01", "2022-06-30"))

# files = ["dataset\ChiNext Shares Only.CSV","dataset\Sci-Tech Innovation Board.CSV","dataset\SSE Mainboard Shares.CSV", "dataset\SZSE Mainboard Shares.CSV"]
files = ["dataset\ChiNext Shares Only.CSV","dataset\SZSE Mainboard Shares.CSV"]

for file in files:
    print(file)
    df = pd.read_csv(file, encoding='cp1252', on_bad_lines='skip')
    file_name = file.split("\\")[1]
    print(file_name)
    tickers = df['Symbol'].unique()
    beforeCovid = {}
    afterCovid  = {}
    for i in tickers:
        print(i)
        number_x = calculate_X(i)

        if number_x >= 10:
            beforeCovid[i] = number_x
            number_x_after = calculate_X_after(i)
            if number_x_after >= 10:
                afterCovid[i] = number_x_after
    print("==============================================================")
    print(beforeCovid)
    print("==============================================================")

    with open("ticker_check_before\\"+file_name, 'w', encoding='cp1252') as f:
        writer = csv.writer(f)
        writer.writerow(["Symbols", "Value"])
        for key, value in beforeCovid.items():
            writer.writerow([key, value])
        
    with open("ticker_check_after\\"+file_name, 'w', encoding='cp1252') as f:
        writer = csv.writer(f)
        writer.writerow(["Symbols", "Value"])
        for key, value in afterCovid.items():
            writer.writerow([key, value])

# print(calculate_X("301349.SZ"))
# df = pd.read_csv('dataset\Sci-Tech Innovation Board.csv', encoding='cp1252',  on_bad_lines='skip')
# print(df)
    # file_name = file.split("\\")[1]