import shioaji as sj
from dotenv import load_dotenv
import os
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

load_dotenv()

def get_api(simulation: bool = True) -> sj.Shioaji:
    api = sj.Shioaji(simulation=simulation)
    api.login(
        api_key=os.environ["API_KEY"],
        secret_key=os.environ["SECRET_KEY"],
    )
    return api
api=get_api()  # 自動登入

stock_code='6515'

kbars = api.kbars(
    contract=api.Contracts.Stocks[stock_code], 
    start=(datetime.now() - timedelta(days=20)).strftime("%Y-%m-%d"), 
    end=datetime.now().strftime("%Y-%m-%d"), 
)

df = pd.DataFrame({**kbars})
df.ts = pd.to_datetime(df.ts)

# df
df = df.groupby(df.ts.dt.date).agg({ "Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum", "Amount": "sum" })
df.drop(columns=["Open", "High", "Low", 'Amount'], inplace=True)
df['code']=stock_code
df['pct'] = df['Close'].pct_change()
df['6pct'] = df['pct'].rolling(6).sum()

df['iterm1_1'] = (df['6pct']*100 > 32).rolling(2).sum() >= 2
df['iterm1_2'] = ((df['6pct']*100 > 25).rolling(2).sum() >= 2) & (df['Close'] - df['Close'].shift(5) >=50)
df['target_pct_1(%)'] = (32-df['pct'].rolling(5).sum()*100).where(df['iterm1_1'])
df['target_pct_2_v1(%)'] = (25-df['pct'].rolling(5).sum()*100).where(df['iterm1_2'])
df['target_pct_2_v2($)'] = (df['Close'].shift(5) + 50).where(df['iterm1_2'])
df['target_info1_1'] = df.apply(
    lambda row: '一定處置' if pd.notna(row['target_pct_1(%)']) and row['target_pct_1(%)'] < -10
                else f"漲超過{row['target_pct_1(%)']:.2f}%以上,價位:{row['Close']*(1+row['target_pct_1(%)']/100):.2f}"
                if pd.notna(row['target_pct_1(%)']) else None,
    axis=1
)

df['target_info1_2'] = df.apply(
    lambda row: '一定處置' if (pd.notna(row['target_pct_2_v1(%)'])) and (row['target_pct_2_v1(%)'] < -10) and (row['target_pct_2_v2($)'] < row['Close']*0.9 )
                else f"漲超過{row['target_pct_2_v1(%)']:.2f}%以上,價位:{max(row['target_pct_2_v2($)'],row['Close']*(1+row['target_pct_2_v1(%)']/100)):.2f}"
                if pd.notna(row['target_pct_2_v1(%)']) else None,
    axis=1
)

df=df[df['iterm1_1'] | df['iterm1_2']][['code','target_info1_1','target_info1_2']]

conn = sqlite3.connect("target_info.db")
df.to_sql("target_info", conn, if_exists="replace", index=True)
conn.close()

if __name__ == "__main__":
    target_info= pd.read_sql("SELECT * FROM target_info", sqlite3.connect("target_info.db"))
    print(target_info)