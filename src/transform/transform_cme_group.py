"""Transform CME Group module"""
import pandas
import os

def transform_market_data(trade_date):
    excel_data_df = pandas.ExcelFile("/private/market-data.xls")
    print(excel_data_df.sheet_names)
    # Take a peek at the first 10 rows of the first tab
    data = excel_data_df.parse(skiprows=4, thousands=",")
    print(data.columns)
    print(data)
    print(data.info)

    df = pandas.DataFrame(data).dropna()
    df = df.rename(columns={"Open OutCry": "OpenOutCry", "Clear Port": "ClearPort", "Open Interest": "OpenInterest"})
    pandas.to_numeric(df.Globex)
    pandas.to_numeric(df.OpenOutCry)
    pandas.to_numeric(df.ClearPort)
    pandas.to_numeric(df.Volume)
    pandas.to_numeric(df.OpenInterest)
    pandas.to_numeric(df.Change)
    df["TradeDate"] = trade_date
    return df
