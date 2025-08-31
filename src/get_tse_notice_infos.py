import requests
import pandas as pd

from datetime import datetime, timedelta
import time
import json

from loguru import logger

def fetch_data() -> pd.DataFrame:
    today = datetime.now()
    query = {
        "querytype": 1,
        "stockNo": "",
        "selectType": "",
        "startDate": (today - timedelta(days=90)).strftime("%Y%m%d"),
        "endDate": today.strftime("%Y%m%d"),
        "sortKind": "STKNO",
        "response": "json",
        "_": time.time() * 1000
    }

    res = requests.get("https://www.twse.com.tw/rwd/zh/announcement/notice", params=query)
    if res.status_code != 200:
        logger.error("Failed to fetch data from TWSE")
        raise Exception("Failed to fetch data from TWSE")
    else:
        logger.info("Data fetched successfully from TWSE")

    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(res.json(), f, ensure_ascii=False, indent=4)
if __name__ == "__main__":
    fetch_data()
