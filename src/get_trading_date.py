from login_api import get_api
import pandas as pd
from datetime import datetime, timedelta
import sqlite3



def convert_date() : 
    date = datetime.now()
    if date.hour<=19 : 
        date = date - timedelta(1)
    return date

def get_kbar(stock_code) :
    kbars = api.kbars(
        contract=api.Contracts.Stocks[stock_code], 
        start=(datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d"), 
        end=convert_date().strftime("%Y-%m-%d"), 
    )
    
    return kbars

def save_to_database(df, db_name="trading_date.db"):
    conn = sqlite3.connect(db_name)
    
    df.to_sql("trading_date", conn, if_exists="replace", index=False)
    conn.close()

api=get_api()  # 自動登入

# 抓出台灣上市股票的交易日（用台積電 2330 當樣板）
try :
    kbars = get_kbar('2330')
    
    trading_date = pd.DataFrame({**kbars})
    trading_date.ts = pd.to_datetime(trading_date.ts)
    trading_date['date'] = trading_date['ts'].dt.date
    trading_date = trading_date.groupby(trading_date.date , as_index=False).agg({"date" : "last"})

    save_to_database(trading_date)
except Exception as e:
    print(e)

if __name__ == "__main__":
    # save_to_database(trading_date)
    # 連接資料庫
    conn = sqlite3.connect("trading_date.db")

    # 讀取整張表（例如 trading_date
    df = pd.read_sql("SELECT * FROM trading_date", conn)

    # 關閉連線
    conn.close()

    # 顯示前幾筆
    print(df)
