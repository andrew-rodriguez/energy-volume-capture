"""Load CME Group module"""
import pymssql


def load(df):
    conn = pymssql.connect(server="localhost", user="sa", password="NaD#7000", database="cmegroup")
    cursor = conn.cursor()

    cursor.execute("truncate table raw_capture;")
    sql = """
        insert raw_capture (name, type, globex, open_outcry, clear_port, volume, open_interest, change, trade_date)
        values (%s, %s, %d, %d, %d, %d, %d, %d, %s);
        """
    for index, row in df.iterrows():
        cursor.execute(sql, (row.Name, row.Type, row.Globex, row.OpenOutCry, row.ClearPort,
                             row.Volume, row.OpenInterest, row.Change, row.TradeDate))
    conn.commit()

    cursor.execute("select * from raw_capture;")
    cursor_row = cursor.fetchone()
    while cursor_row:
        print(cursor_row)
        cursor_row = cursor.fetchone()
    cursor.close()
