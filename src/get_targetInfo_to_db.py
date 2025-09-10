from login_api import get_api
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

api = get_api()

##############抓取最新日期##############
db_path="trading_date.db"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

cursor.execute(f"""
    SELECT date FROM trading_date
    ORDER BY date DESC
""")
latest_date = cursor.fetchone()[0]
conn.close()
######################################
def get_kbar(stock_code) :
    kbars = api.kbars(
        contract=api.Contracts.Stocks[stock_code], 
        start=(datetime.now() - timedelta(days=200)).strftime("%Y-%m-%d"), 
        end=latest_date, 
    )
    
    return kbars

def target_info_to_db(stock_code) :
    ##############抓取stock_info##############
    conn = sqlite3.connect("stock_info.db")
    cursor = conn.cursor()
    cursor.execute(f"""
                SELECT `type`
                FROM taiwan_stock_info
                WHERE `stock_id` = {stock_code}
                """
    )
    
    source = cursor.fetchone()[0] # twse or tpex
    conn.close()
    if source is None :
        print("資料源不明")
    ##############抓取最新target_info##############
    with sqlite3.connect("target_info.db") as conn:
        cursor = conn.cursor()
        # 先檢查資料庫裡有沒有這張表
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", ("target_info",))
        exists = cursor.fetchone()
        if exists :
            cursor.execute(f""" SELECT * FROM target_info 
                            WHERE code = {stock_code} 
                            AND ts = '{latest_date}' 
                            """ )
            data = cursor.fetchall()[-1:]
            if data:
                return
        print(f"fetch {stock_code}")
        kbars=get_kbar(stock_code)
        
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
        if source == "twse" :
            iterm1_1_param = 32
            iterm1_2_v1_param = 25
            iterm1_2_v2_param = 50
        elif source == "tpex" :
            iterm1_1_param = 30
            iterm1_2_v1_param = 23
            iterm1_2_v2_param = 40
        iterm1_1 = (df['6pct']*100 > iterm1_1_param)
        iterm1_2 = (df['6pct']*100 > iterm1_2_v1_param) & (df['Close'] - df['Close'].shift(5) >=iterm1_2_v2_param)
        ############################################################第二款############################################################
        iterm2_1 = (df['30pct'] * 100 >100) 
        iterm2_2 = (df['60pct'] * 100 >130) 
        iterm2_3 = (df['90pct'] * 100 >160) 
        item2_all = (df['Close']>df["Close"].shift(1))
        ############################################################第三款############################################################
        if source == "twse" :
            iterm3_param = 25
        elif source == "tpex" :
            iterm3_param = 27
        
        iterm3 = (df['6pct'] * 100 > iterm3_param) & (df['Volume'] > df['60volume'] * 5 )
        ############################################################注意條件############################################################
        # df['notice_only1']= (iterm1_1 | iterm1_2) 
        # df['notice_1~8']= (iterm1_1 | iterm1_2) | ((iterm2_1 | iterm2_2 | iterm2_3) & item2_all) | (iterm3)
        # ############################################################待處置條件############################################################            
        # df['pre_punished_only1'] = (df['notice_only1'].rolling(2).sum()==2) & (df['notice_only1'].rolling(3).sum()!=3) 
        # df['pre_punished_1~8'] = (df['notice_1~8'].rolling(4).sum()==4) & (df['notice_1~8'].rolling(5).sum()!=5) | (df['notice_1~8'].rolling(9).sum()==5) | (df['notice_1~8'].rolling(29).sum()==11) 
        ############################################################進處置標準############################################################  
        ####################第一款標準####################
        df['target_pct1_1(%)'] = (iterm1_1_param-df['pct'].rolling(5).sum()*100)#.where(df['pre_punished_only1'] | df['pre_punished_1~8'])
        df['target_pct1_2_v1(%)'] = (iterm1_2_v1_param-df['pct'].rolling(5).sum()*100)#.where(df['pre_punished_only1'] | df['pre_punished_1~8'])
        df['target_pct1_2_v2($)'] = (df['Close'].shift(4) + iterm1_2_v2_param)#.where(df['pre_punished_only1'] | df['pre_punished_1~8'])

        df['target_info1_1(%)'] = df.apply(
            lambda row: "1-1 : 一定處置" if pd.notna(row['target_pct1_1(%)']) and row['target_pct1_1(%)'] < -10
                    else "1-1 : 不會符合" if pd.notna(row['target_pct1_1(%)']) and row['target_pct1_1(%)'] > 10
                    else f"1-1 : 漲幅 > {row['target_pct1_1(%)']:.2f}%,價位 : {row['Close']*(1+row['target_pct1_1(%)']/100):.2f}" if pd.notna(row['target_pct1_1(%)'])
                    else None,
            axis=1
        )

        df['target_info1_2(%+N)'] = df.apply(
            lambda row: '1-2 : 一定處置' if (pd.notna(row['target_pct1_2_v1(%)'])) and (row['target_pct1_2_v1(%)'] < -10) and (row['target_pct1_2_v2($)'] < row['Close']*0.9 )
                        else "1-2 : 不會符合" if pd.notna(row['target_pct1_2_v1(%)']) and ((row['target_pct1_2_v1(%)'] > 10) or (row['target_pct1_2_v2($)'] > row['Close']*1.1 )) 
                        else f"1-2 : 漲幅 > {row['target_pct1_2_v1(%)']:.2f}% 且股價 > {max(row['target_pct1_2_v2($)'],row['Close']*(1+row['target_pct1_2_v1(%)']/100)):.2f}"
                        if pd.notna(row['target_pct1_2_v1(%)']) 
                        else None,
            axis=1
        )
        ####################第二款標準####################
        df['target_pct2_1(%)'] = (100-df['Close'].pct_change(28)*100)#.where(df['pre_punished_1~8'])
        df['target_pct2_2(%)'] = (130-df['Close'].pct_change(58)*100)#.where(df['pre_punished_1~8'])
        df['target_pct2_3(%)'] = (160-df['Close'].pct_change(88)*100)#.where(df['pre_punished_1~8'])

        df['target_pct_2($)'] = df['Close']#.where(df['pre_punished_1~8'])

        df['target_info2(%)'] = df.apply(
            lambda row: '2-1 : 一定達標' if pd.notna(row['target_pct2_1(%)']) and min(row['target_pct2_1(%)'],row['target_pct2_2(%)'],row['target_pct2_3(%)']) < -10
                        else '2-1 : 不會達標' if pd.notna(row['target_pct2_1(%)']) and min(row['target_pct2_1(%)'],row['target_pct2_2(%)'],row['target_pct2_3(%)']) > 10
                        else f"2-1 : 漲幅 > {min(row['target_pct2_1(%)'],row['target_pct2_2(%)'],row['target_pct2_3(%)']):.2f}%"
                        if pd.notna(row['target_pct2_1(%)']) 
                        else None,
            axis=1
        )

        df['target_info2($)'] = df.apply(
            lambda row: f"2-2 : 股價 > {row['target_pct_2($)']:.2f}"
                        if pd.notna(row['target_pct_2($)']) else None,
            axis=1
        )
        ####################第三款標準####################
        df['target_pct3_v1(%)'] = (iterm3_param-df['pct'].rolling(5).sum()*100)#.where(df['pre_punished_1~8'])

        # (59v'+v)/60 *5 < v => 59/11v' < v
        df['target_pct3_v2(volume)'] = (df['Volume'].rolling(59).sum()/11)#.where(df['pre_punished_1~8'])

        df['target_info3(%)'] = df.apply(
            lambda row: '3-1 : 一定達標' if pd.notna(row['target_pct3_v1(%)']) and row['target_pct3_v1(%)'] < -10
                        else '3-1 : 不會達標' if pd.notna(row['target_pct3_v1(%)']) and row['target_pct3_v1(%)'] > 10
                        else f"3-1 : 漲幅 > {row['target_pct3_v1(%)']:.2f}% , 股價 : { row['Close']*(1+row['target_pct3_v1(%)']/100):.2f}"
                        if pd.notna(row['target_pct3_v1(%)']) else None,
            axis=1
        )

        df['target_info3(volume)'] = df.apply(
            lambda row: f"3-2 : 量 > {int(row['target_pct3_v2(volume)'])}"
                        if pd.notna(row['target_pct3_v2(volume)']) 
                        else None,
            axis=1
        )

        df = df[['code','target_info1_1(%)','target_info1_2(%+N)','target_info2(%)','target_info2($)','target_info3(%)','target_info3(volume)']].tail(1)

        df.to_sql("target_info", conn, if_exists="append", index=True)

if __name__ == "__main__":
    target_info= pd.read_sql("SELECT * FROM target_info", sqlite3.connect("target_info.db"))
    # target_info = target_info[target_info['code']=='4976']
    target_info = target_info[target_info['ts']== (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")]
    print(target_info)