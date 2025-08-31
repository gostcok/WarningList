from loguru import logger
import json
import pandas as pd
import sqlite3

logger.add("logs/{time:YYYY-MM-DD}.log", rotation="00:00", retention="7 days", encoding="utf-8")


with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

data = pd.DataFrame(data["data"], columns=data["fields"])
data["證券代號"] = data["證券代號"].astype(str)
data = data[data["證券代號"].str.len() == 4]
data = data[data["證券代號"] > "1000"]

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

data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: [refs[item.split("﹝")[1]] for item in x.split("﹞")[:-1]])
def convert_date(date_str):
    year = int(date_str[:3]) + 1911
    month = int(date_str[4:6])
    day = int(date_str[7:9])
    return f"{year}-{month:02d}-{day:02d}"
data["日期"] = data["日期"].apply(convert_date)

def save_to_database(df, db_name="stocks.db"):
    conn = sqlite3.connect(db_name)
    df.to_sql("stocks", conn, if_exists="replace", index=False)
    conn.close()

# Convert the list in "注意交易資訊" to a JSON string before saving to the database
data["注意交易資訊"] = data["注意交易資訊"].apply(lambda x: json.dumps(x, ensure_ascii=False))

# Save DataFrame to SQLite database
# save_to_database(data)
if __name__ == "__main__":
    print(data)
