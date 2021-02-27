"""Extract CME Group module"""
import requests
import os
import pandas


def get_market_data(trade_date):
    print(trade_date.strftime("%Y%m%d"))
    url = "https://www.cmegroup.com/CmeWS/exp/voiProductsViewExport.ctl?media=xls&tradeDate=" \
          + trade_date.strftime("%Y%m%d") + "&assetClassId=7&reportType=P&excluded=CEE,CEU,KCB"
    response = requests.get(url, stream=True)  # create HTTP response object
    with open(os.path.dirname(os.path.realpath(__file__)) + "/../../downloads/market-data.xls", 'wb') as f:
        # Saving received content as a png file in binary format
        # write the contents of the response (r.content) to a new file in binary mode.
        f.write(response.content)
    excel_data_df = pandas.ExcelFile(response.content)
    print(excel_data_df.sheet_names)
