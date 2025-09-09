from FinMind.data import DataLoader
import sqlite3
from datetime import datetime
import pandas as pd

def save_db() :
    # 初始化
    dl = DataLoader()

    # 下載台股基本資料
    df = dl.taiwan_stock_info()
    dl.taiwan_stock_daily
    df = df.sort_values("stock_id")
    df = df.drop_duplicates(subset="stock_id", keep="first")
    df = df[~df["industry_category"].isin(["ETF", "大盤", "Index"])]
    df = df[df["type"].isin(["twse", "tpex"])]
    df["stock_id"] = df["stock_id"].astype(str)
    df = df[df["stock_id"].str.len() == 4]
    df = df[['stock_id','stock_name','type','date']]

    # 存到 SQLite
    conn = sqlite3.connect("stock_info.db")
    df.to_sql("taiwan_stock_info", conn, if_exists="replace", index=False)
    conn.close()
    print("stock_info fetch successfully")

if __name__ == "__main__":
    save_db()
    stock_info= pd.read_sql("SELECT * FROM taiwan_stock_info", sqlite3.connect("stock_info.db"))
    print(stock_info)

