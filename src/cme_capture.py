"""CME Capture module"""
import pandas as pd
import datetime
import requests
import pymssql
import json
import os


TRADE_DATE = datetime.datetime(2021, 2, 18)
BASE_URL = 'https://www.cmegroup.com/CmeWS/exp/voiProductsViewExport.ctl?media=xls&tradeDate=' \
           + TRADE_DATE.strftime('%Y%m%d') + '&assetClassId=7&reportType=P&excluded=CEE,CEU,KCB'


def extract_data(url=BASE_URL) -> pd.DataFrame:
    print('Extracting data...' + url)
    response = requests.get(url, stream=True)
    df = pd.ExcelFile(response.content)
    return df


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    print('Transforming data...')
    df = df.parse(skiprows=4, thousands=',')
    df = pd.DataFrame(df).dropna()
    df = df.rename(columns={'Open OutCry': 'OpenOutCry', 'Clear Port': 'ClearPort', 'Open Interest': 'OpenInterest'})
    pd.to_numeric(df.Globex)
    pd.to_numeric(df.OpenOutCry)
    pd.to_numeric(df.ClearPort)
    pd.to_numeric(df.Volume)
    pd.to_numeric(df.OpenInterest)
    pd.to_numeric(df.Change)
    df['TradeDate'] = TRADE_DATE
    return df


def load_data(df: pd.DataFrame) -> None:
    print('Loading data...')
    conn = pymssql.connect(server='localhost', user='sa', password=password('cmegroup'), database='cmegroup')
    cursor = conn.cursor()

    cursor.execute('truncate table raw_capture;')
    sql = '''
        insert raw_capture (name, type, globex, open_outcry, clear_port, volume, open_interest, change, trade_date)
        values (%s, %s, %d, %d, %d, %d, %d, %d, %s);
        '''
    for index, row in df.iterrows():
        cursor.execute(sql, (row.Name, row.Type, row.Globex, row.OpenOutCry, row.ClearPort,
                             row.Volume, row.OpenInterest, row.Change, row.TradeDate))
    conn.commit()

    cursor.execute('delete from CMEEnergyVolumes where trade_date = %s', TRADE_DATE.date())
    cursor.execute('insert into CMEEnergyVolumes select * from raw_capture where trade_date = %s', TRADE_DATE.date())
    conn.commit()

    cursor.execute('select * from CMEEnergyVolumes where trade_date = %s', TRADE_DATE.date())
    cursor_row = cursor.fetchone()
    while cursor_row:
        print(cursor_row)
        cursor_row = cursor.fetchone()
    cursor.close()


def password(key) -> str:
    with open(os.path.dirname(os.path.realpath(__file__)) + '/../private/secret.json', 'r') as pwdfile:
        data = pwdfile.read()
    obj = json.loads(data)
    return obj[key]


def etl():
    df = extract_data()
    df = transform_data(df)
    load_data(df)


if __name__ == '__main__':
    etl()


#
# trade_date = datetime.datetime(2021, 2, 18)
#
# print(extract_cme_group.__doc__)
# extract_cme_group.get_market_data(trade_date)
#
# print(transform_cme_group.__doc__)
# df = transform_cme_group.transform_market_data(trade_date)
#
# print(load_cme_group.__doc__)
# load_cme_group.load(df)
