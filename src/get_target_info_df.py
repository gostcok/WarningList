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

stock_code='8021'

kbars = api.kbars(
    contract=api.Contracts.Stocks[stock_code], 
    start=(datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d"), 
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

df['30pct'] = df['Close'].pct_change(29)
df['60pct'] = df['Close'].pct_change(59)
df['90pct'] = df['Close'].pct_change(89)

df['60volume'] = df['Volume'].rolling(60).mean()
############################################################第一款############################################################
iterm1_1 = (df['6pct']*100 > 32)
iterm1_2 = (df['6pct']*100 > 25) & (df['Close'] - df['Close'].shift(5) >=50)
############################################################第二款############################################################
iterm2_1 = (df['30pct'] * 100 >100) 
iterm2_2 = (df['60pct'] * 100 >130) 
iterm2_3 = (df['90pct'] * 100 >160) 
item2_all = (df['Close']>df["Close"].shift(1))
############################################################第三款############################################################
iterm3 = (df['6pct'] * 100 > 25) & (df['Volume'] > df['60volume'] * 5 )
############################################################注意條件############################################################
df['notice_only1']= (iterm1_1 | iterm1_2) 
df['notice_1~8']= (iterm1_1 | iterm1_2) | ((iterm2_1 | iterm2_2 | iterm2_3) & item2_all) | (iterm3)
############################################################待處置條件############################################################            
df['pre_punished_only1'] = (df['notice_only1'].rolling(2).sum()==2) & (df['notice_only1'].rolling(3).sum()!=3) 
df['pre_punished_1~8'] = (df['notice_1~8'].rolling(4).sum()==4) & (df['notice_1~8'].rolling(5).sum()!=5) | (df['notice_1~8'].rolling(9).sum()==5) | (df['notice_1~8'].rolling(29).sum()==11) 
############################################################進處置標準############################################################  
####################第一款標準####################
df['target_pct1_1(%)'] = (32-df['pct'].rolling(5).sum()*100).where(df['pre_punished_only1'] | df['pre_punished_1~8'])
df['target_pct1_2_v1(%)'] = (25-df['pct'].rolling(5).sum()*100).where(df['pre_punished_only1'] | df['pre_punished_1~8'])
df['target_pct1_2_v2($)'] = (df['Close'].shift(4) + 50).where(df['pre_punished_only1'] | df['pre_punished_1~8'])

df['target_info1_1(%)'] = df.apply(
    lambda row: '一定處置' if pd.notna(row['target_pct1_1(%)']) and row['target_pct1_1(%)'] < -10
                else f"漲超過{row['target_pct1_1(%)']:.2f}%以上,價位:{row['Close']*(1+row['target_pct1_1(%)']/100):.2f}"
                if pd.notna(row['target_pct1_1(%)']) else None,
    axis=1
)

df['target_info1_2(%+N)'] = df.apply(
    lambda row: '一定處置' if (pd.notna(row['target_pct1_2_v1(%)'])) and (row['target_pct1_2_v1(%)'] < -10) and (row['target_pct1_2_v2($)'] < row['Close']*0.9 )
                else f"漲超過{row['target_pct1_2_v1(%)']:.2f}%以上,價位:{max(row['target_pct1_2_v2($)'],row['Close']*(1+row['target_pct1_2_v1(%)']/100)):.2f}"
                if pd.notna(row['target_pct1_2_v1(%)']) else None,
    axis=1
)
####################第二款標準####################
df['target_pct2_1(%)'] = (100-df['Close'].pct_change(28)*100).where(df['pre_punished_1~8'])
df['target_pct2_2(%)'] = (130-df['Close'].pct_change(58)*100).where(df['pre_punished_1~8'])
df['target_pct2_3(%)'] = (160-df['Close'].pct_change(88)*100).where(df['pre_punished_1~8'])

df['target_pct_2($)'] = df['Close'].where(df['pre_punished_1~8'])

df['target_info2(%)'] = df.apply(
    lambda row: '起訖漲幅一定達標' if pd.notna(row['target_pct2_1(%)']) and max(row['target_pct2_1(%)'],row['target_pct2_2(%)'],row['target_pct2_3(%)']) < -10
                else f"漲超過{min(row['target_pct2_1(%)'],row['target_pct2_2(%)'],row['target_pct2_3(%)']):.2f}%以上"
                if pd.notna(row['target_pct2_1(%)']) else None,
    axis=1
)

df['target_info2($)'] = df.apply(
    lambda row: f"價位:{row['target_pct_2($)']:.2f}"
                if pd.notna(row['target_pct_2($)']) else None,
    axis=1
)
####################第三款標準####################
df['target_pct3_v1(%)'] = (25-df['pct'].rolling(5).sum()*100).where(df['pre_punished_1~8'])

# (59v'+v)/60 *5 < v => 59/11v' < v
df['target_pct3_v2(volume)'] = (df['Volume'].rolling(59).sum()/11).where(df['pre_punished_1~8'])

df['target_info3(%)'] = df.apply(
    lambda row: '累積漲幅一定達標' if pd.notna(row['target_pct3_v1(%)']) and row['target_pct3_v1(%)'] < -10
                else f"漲超過{row['target_pct3_v1(%)']:.2f}%以上,價位:{ row['Close']*(1+row['target_pct3_v1(%)']/100):.2f}"
                if pd.notna(row['target_pct3_v1(%)']) else None,
    axis=1
)

df['target_info3(volume)'] = df.apply(
    lambda row: f"量:{int(row['target_pct3_v2(volume)'])}"
                if pd.notna(row['target_pct3_v2(volume)']) else None,
    axis=1
)

df = df[df['pre_punished_only1'] | df['pre_punished_1~8']][['code','pre_punished_only1','pre_punished_1~8', 'target_info1_1(%)','target_info1_2(%+N)','target_info2(%)','target_info2($)','target_info3(%)','target_info3(volume)']]


conn = sqlite3.connect("target_info.db")
df.to_sql("target_info", conn, if_exists="replace", index=True)
conn.close()

if __name__ == "__main__":
    target_info= pd.read_sql("SELECT * FROM target_info", sqlite3.connect("target_info.db"))
    print(target_info)