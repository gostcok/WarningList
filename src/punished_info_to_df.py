from loguru import logger
import json
import pandas as pd
import sqlite3
import re
import get_stock_punished_info

def convert_date(date_str):
    start_date, end_date = date_str.split("～")
    start_year = int(start_date[:3]) + 1911
    start_month = int(start_date[4:6])
    start_day = int(start_date[7:9])
    
    end_year = int(end_date[:3]) + 1911
    end_month = int(end_date[4:6])
    end_day = int(end_date[7:9])
    return f"{start_year}-{start_month:02d}-{start_day:02d}～{end_year}-{end_month:02d}-{end_day:02d}"

def save_to_database(df, db_name="punished_stocks.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("stocks", conn, if_exists="replace", index=False)
    conn.close()

logger.add("logs/{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", encoding="utf-8")
# from 1 to 13
refs = {
    "第一款": 1,
    "第二款": 2,
    "第三款": 3,
    "第四款": 4,
    "第五款": 5,
    "第六款": 6,
    "第七款": 7,
    "第八款": 8,
    "第九款": 9,
    "第十款": 10,
    "第十一款": 11,
    "第十二款": 12,
    "第十三款": 13
}
datas=[]

with open("TSE_punished.json", "r", encoding="utf-8") as f:
    TSE_data = json.load(f)
    datas.append(TSE_data)
with open("OTC_punished.json", "r", encoding="utf-8") as f:
    OTC_data = json.load(f)
    datas.append(OTC_data)

for idx, data_source in enumerate(datas):
    if data_source == TSE_data:
        data = pd.DataFrame(data_source["data"], columns=data_source["fields"])
        data.drop(columns=['處置措施','處置內容','備註'], inplace=True)
        data['Source'] = 'TSE'
    if data_source == OTC_data:
        data = pd.DataFrame(data_source['tables'][0]["data"], columns=data_source['tables'][0]["fields"])
        data['證券名稱'] = data['證券名稱'].str.split('(').str.get(0)
        data.drop(columns=['收盤價','本益比',' ','處置內容'], inplace=True)
        data.rename(columns={
            "處置原因": "處置條件",
            "處置起訖時間": "處置起迄時間"
        }, inplace=True)
        data['Source'] = 'OTC'
        data=data[['編號', '公布日期', '證券代號', '證券名稱', '累計', '處置條件', '處置起迄時間', 'Source']]
        

    data["證券代號"] = data["證券代號"].astype(str)
    data = data[data["證券代號"].str.len() == 4]
    data = data[data["證券代號"] > "1000"]

    # if data_source == TSE_data:
    #     data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: [refs[item.split("﹝")[1]] for item in x.split("﹞")[:-1]])
    # elif data_source == OTC_data:
    #     data["注意交易資訊"] = data["注意交易資訊"].apply(
    #         lambda x: [
    #             refs[m]
    #             for m in re.findall(r'[（(](第[一二三四五六七八九十百零]+款)[)）]', str(x))
    #             if m in refs
    #         ]
    #     )
    #     data = data.drop(columns=['link'])

    data['處置起迄時間'] = data['處置起迄時間'].str.replace("~", "～")  # 半形轉全形
    
    data['處置起迄時間']=data['處置起迄時間'].apply(convert_date)

    data[['處置起始時間', '處置結束時間']] = data['處置起迄時間'].str.split('～', n=1, expand=True)
    
    # data = data.sort_values(by="處置結束時間", ascending=True)
    # # Convert the list in "注意交易資訊" to a JSON string before saving to the database
    # data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: json.dumps(x, ensure_ascii=False))

    conn = sqlite3.connect("punished_stocks.db")
    if idx == 0:
        # 第一次直接清空（replace）
        data.to_sql("stocks", conn, if_exists="replace", index=False)
    else:
        # 之後都用 append
        data.to_sql("stocks", conn, if_exists="append", index=False)
    conn.close()
    
if __name__ == "__main__":
    df= pd.read_sql("SELECT * FROM stocks", sqlite3.connect("punished_stocks.db"))
    print(df)

