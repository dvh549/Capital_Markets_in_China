# General / Common libs
import pandas as pd
from pandas_datareader import data as pdr
import yfinance as yf
yf.pdr_override()
import pyfolio as pf
import csv 
import glob
from joblib import Parallel, delayed
from yahoo_fin import stock_info as si

def calculate_X(ticker):
    try:
        ticker_data = pdr.get_data_yahoo(ticker, start="2012-01-01", end="2022-06-30")
        ticker_close = ticker_data["Close"]
        # if pd.to_datetime(ticker_close.keys()[0])==pd.to_datetime("2009-01-02"):
        if ticker_close.head(1).values[0]> ticker_close.tail(1).values[0]:
            return -1*round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
        return round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
    except:
        return 0

def calculate_X_after(ticker):
    try:
        ticker_data = pdr.get_data_yahoo(ticker, start="2020-01-01", end="2022-06-30")
        ticker_close =ticker_data["Close"]
        # if pd.to_datetime(ticker_close.keys()[0])==pd.to_datetime("2020-01-02"):
        print((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
        if ticker_close.head(1).values[0]> ticker_close.tail(1).values[0]:
            return -1* round((ticker_close.tail(1).values / ticker_close.head(1).values)[0], 3)
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
    perf_dict = pf.show_perf_stats(ticker_return_ts).Backtest.to_dict()
    perf_dict["Symbols"] = ticker
    return perf_dict

def get_ticker_ratios(ticker):
    curr_ticker = yf.Ticker(ticker).stats()
    financial_ratios = curr_ticker["financialData"]
    financial_ratios["Symbols"] = ticker
    return financial_ratios

# print(calculate_X_after("600201.SS"))
# print(get_ticker_stats("AAPL", "2020-01-01", "2022-06-30"))

# files = ["dataset\ChiNext Shares Only.CSV", "dataset\SZSE Mainboard Shares.CSV"]
# files = ["dataset\Sci-Tech Innovation Board.CSV","dataset\SSE Mainboard Shares.CSV"]
# DOW.csv has no 10x stocks, NASDAQ & SP500 do.
# files = ["original_datasets/SP500.csv"]
files = ["original_datasets/SGX.csv"]

def check_10x_and_industry(symbol):
    multiplier = calculate_X(symbol)
    print(multiplier)
    if multiplier >= 10:
        try:
            industry = yf.Ticker(symbol).stats()["summaryProfile"]["industry"]
            return symbol, multiplier, industry
        except:
            return symbol, multiplier, ""

for file in files:
    df, file_name  = pd.read_csv(file), file.split("/")[1]
    # data = Parallel(n_jobs=4, verbose=32)(delayed(check_10x_and_industry)() for symbol in df["Symbols"])
    data = Parallel(n_jobs=4, verbose=32)(delayed(check_10x_and_industry)(f"{symbol}.SI") for symbol in df["Symbol"])
    data = [data_point for data_point in data if data_point is not None]
    shortlisted_df = pd.DataFrame(data, columns=["Symbol", "Multiplier", "Industry"])
    shortlisted_df.to_csv(f"ticker_first_screening/{file_name}", index=False)

# for file in files:
#     print(file)
#     df = pd.read_csv(file, encoding='cp1252', on_bad_lines='skip')
#     file_name = file.split("\\")[1]
#     print(file_name)
#     tickers = df['Symbol'].unique()
#     beforeCovid = {}
#     afterCovid  = {}
#     screen = {}
#     for i in tickers:
#         # print(i)
#         curr_ticker = i
#         # curr_ticker = i.split(".")[0]+".SS"
#         print(curr_ticker)
#         number_x = calculate_X(curr_ticker)
#         # number_x = calculate_X(i)

#         if number_x >= 10:
#             screen[i] = number_x
            # number_x_after = calculate_X_after(curr_ticker)
            # print(i)
            # if number_x_after >= 10:
            # afterCovid[i] = number_x_after
    # print("==============================================================")
    # print(beforeCovid)
    # print("==============================================================")

    # print("==============================================================")
    # print(screen)
    # print("==============================================================")

    # print(afterCovid)
    # print("==============================================================")

    # with open("ticker_check_before\\"+file_name, 'w', encoding='cp1252') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["Symbols", "BeforeCovid", "DuringCovid"])
    #     for key, value in beforeCovid.items():
    #         writer.writerow([key, value, afterCovid[key] ])

    # with open("ticker_first_screening\\"+file_name, 'w', encoding='cp1252') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["Symbols", "Multiplier"])
    #     for key, value in screen.items():
    #         writer.writerow([key, value])
        
    # with open("ticker_check_after\\"+file_name, 'w', encoding='cp1252') as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["Symbols", "Value"])
    #     for key, value in afterCovid.items():
    #         writer.writerow([key, value])

# print(calculate_X("301349.SZ"))
# df = pd.read_csv('dataset\Sci-Tech Innovation Board.csv', encoding='cp1252',  on_bad_lines='skip')
# print(df)
    # file_name = file.split("\\")[1]

path = "ticker_first_screening"
filtered_csv_files = glob.glob(f"{path}/*.csv")

# for filtered_csv in filtered_csv_files:
#     df, file_name = pd.read_csv(filtered_csv), filtered_csv.split("\\")[1]
#     symbols = df["Symbols"]
#     before, during = [], []
#     for symbol in symbols:
#         before.append(get_ticker_stats(symbol, start="2009-01-01", end="2019-12-31"))
#         during.append(get_ticker_stats(symbol, start="2020-01-01", end="2022-06-30"))
#     before_df, during_df = pd.DataFrame(before), pd.DataFrame(during)
#     final_df = pd.concat([df.loc[:, ["Symbols", "BeforeCovid"]], before_df, df["DuringCovid"], during_df], join="inner", axis=1).iloc[:, :-1]
#     final_df.to_csv(f"finalised_csv_files/{file_name}")

# for filtered_csv in filtered_csv_files:
#     df, file_name = pd.read_csv(filtered_csv), filtered_csv.split("\\")[1]
#     symbols = df["Symbols"]
#     stats = []
#     for symbol in symbols:
#         stats.append(get_ticker_stats(symbol, start="2012-01-01", end="2022-06-30"))
#     stats_df = pd.DataFrame(stats)
#     final_df = pd.concat([df, stats_df], join="inner", axis=1).iloc[:, :-1]
#     final_df.to_csv(f"finalised_csv_files/first_screening_{file_name}")

# files = ["ticker_first_screening/NASDAQ.csv", "ticker_first_screening/SP500.csv"]

# for file in files:
#     df, file_name = pd.read_csv(file), file.split("/")[1]
#     ratios = Parallel(n_jobs=4, verbose=32)(delayed(get_ticker_ratios)(symbol) for symbol in df["Symbol"])
#     ratios_df = pd.DataFrame.from_dict(ratios)
#     ratios_df.to_csv(f"finalised_csv_files/ratios_{file_name}", index=False)

def process_ticker(ticker):
    try:
        industry = yf.Ticker(ticker).stats()["summaryProfile"]["industry"]
        print(ticker)
        # return industry
        return ticker, industry
    except:
        print("Cannot find.")
        return

def process_to_dict(industry_list):
    industries = {}
    for industry in industry_list:
        if industry not in industries:
            industries[industry] = 1
        else:
            industries[industry] += 1
    return list(industries.items())
                
# def process_csv(filtered_csv):
# # for filtered_csv in filtered_csv_files:
#     # req_cols = ["Symbol", "Date"]
#     req_cols = ["Symbols"]
#     # df, file_name = pd.read_csv(filtered_csv, encoding='cp1252', on_bad_lines='skip', usecols=req_cols), filtered_csv.split("\\")[1]
#     df, file_name = pd.read_csv(filtered_csv, encoding='cp1252', on_bad_lines='skip', usecols=req_cols), filtered_csv.split("\\")[1]
#     # df = df[df["Date"] >= "2012-01-01"]
#     # symbols, industry_list = df['Symbol'].unique(), []
#     symbols, industry_list = df["Symbols"], []
#     industry_list = Parallel(n_jobs=4, verbose=32)(delayed(process_ticker)(symbol) for symbol in symbols)
#     # industries = process_to_dict(industry_list)
#     # industries_df = pd.DataFrame(industries, columns=["Industry", "Total Count"])
#     industries_df = pd.DataFrame(industry_list, columns=["Symbol", "Industry"])
#     # industries_df.to_csv(f"finalised_csv_files/industries_{file_name}", index=False)
#     industries_df.to_csv(f"ticker_first_screening_ratios/industries_{file_name}", index=False)

# Parallel(n_jobs=3, verbose=32)(delayed(process_csv)(filtered_csv) for filtered_csv in filtered_csv_files)

# def get_and_save_US_Companies():
#     us_stock_list = [pd.DataFrame(si.tickers_sp500()), pd.DataFrame(si.tickers_nasdaq()), pd.DataFrame(si.tickers_dow())]
#     us_stock_names = ["SP500.csv", "NASDAQ.csv", "DOW.csv"]
#     for i in range(len(us_stock_list)):
#         df, file_name = us_stock_list[i], us_stock_names[i]
#         df.to_csv(f"original_dataset/{file_name}", index=False)
#     return "Done."

# get_and_save_US_Companies()