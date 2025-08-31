from FinMind.data import DataLoader
import pandas as pd
import sqlite3
import datetime
# 設定 FinMind 無登入也可以使用（但有速率限制）
api = DataLoader()

# 抓出台灣上市股票的交易日（用台積電 2330 當樣板）
df = api.taiwan_stock_daily(
    stock_id="2330",
    start_date="2010-01-01",
    end_date=datetime.datetime.now().strftime("%Y-%m-%d")
)

# 篩出交易日
trading_date = pd.DataFrame({"date": pd.to_datetime(df["date"].unique())})
trading_date = trading_date.sort_values("date").reset_index(drop=True)
trading_date['date'] = trading_date['date'].dt.date

def save_to_database(df, db_name="trading_date.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("trading_date", conn, if_exists="replace", index=False)
    conn.close()
save_to_database(trading_date)
if __name__ == "__main__":
    save_to_database(trading_date)
    # 連接資料庫
    conn = sqlite3.connect("trading_date.db")

    # 讀取整張表（例如 trading_date
    df = pd.read_sql("SELECT * FROM trading_date", conn)

    # 關閉連線
    conn.close()

    # 顯示前幾筆
    print(df.tail())
