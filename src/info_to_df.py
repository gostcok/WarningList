from loguru import logger
import json
import pandas as pd
import sqlite3
import re

def convert_date(date_str):
    year = int(date_str[:3]) + 1911
    month = int(date_str[4:6])
    day = int(date_str[7:9])
    return f"{year}-{month:02d}-{day:02d}"

def save_to_database(df, db_name="stocks.db"):
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

with open("TSE_notice.json", "r", encoding="utf-8") as f:
    TSE_data = json.load(f)
    datas.append(TSE_data)
with open("OTC_notice.json", "r", encoding="utf-8") as f:
    OTC_data = json.load(f)
    datas.append(OTC_data)

for idx, data_source in enumerate(datas):
    if data_source == TSE_data:
        data = pd.DataFrame(data_source["data"], columns=data_source["fields"])
        data['Source'] = 'TSE'
    elif data_source == OTC_data:
        data = pd.DataFrame(data_source['tables'][0]["data"], columns=data_source['tables'][0]["fields"])
        data.rename(columns={
            "累計": "累計次數",
            "公告日期": "日期"
        }, inplace=True)
        data['Source'] = 'OTC'

    data["證券代號"] = data["證券代號"].astype(str)
    data["累計次數"] = data["累計次數"].astype(object)
    data = data[data["證券代號"].str.len() == 4]
    data = data[data["證券代號"] > "1000"]

    if data_source == TSE_data:
        data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: [refs[item.split("﹝")[1]] for item in x.split("﹞")[:-1]])
    elif data_source == OTC_data:
        data["注意交易資訊"] = data["注意交易資訊"].apply(
            lambda x: [
                refs[m]
                for m in re.findall(r'[（(](第[一二三四五六七八九十百零]+款)[)）]', str(x))
                if m in refs
            ]
        )
        data = data.drop(columns=['link'])

    data["日期"] = data["日期"].apply(convert_date)
    # Convert the list in "注意交易資訊" to a JSON string before saving to the database
    data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: json.dumps(x, ensure_ascii=False))

    conn = sqlite3.connect("stocks.db")
    if idx == 0:
        # 第一次直接清空（replace）
        data.to_sql("stocks", conn, if_exists="replace", index=False)
    else:
        # 之後都用 append
        data.to_sql("stocks", conn, if_exists="append", index=False)
    conn.close()
    
if __name__ == "__main__":
    df= pd.read_sql("SELECT * FROM stocks", sqlite3.connect("stocks.db"))
    print(df)
