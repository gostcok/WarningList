import requests
import pandas as pd

from datetime import datetime, timedelta
import time
import json

from loguru import logger

def fetch_data() -> pd.DataFrame:
    TSE_URL = "https://www.twse.com.tw/rwd/zh/announcement/notice"
    OTC_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"

    today = datetime.now()
    start_date = (today - timedelta(days=90))
    
    TSE_query = {
        "querytype": 1,
        "stockNo": "",
        "selectType": "",
        "startDate": start_date.strftime("%Y%m%d"),
        "endDate": today.strftime("%Y%m%d"),
        "sortKind": "STKNO",
        "response": "json",
        "_": time.time() * 1000
    }
    OTC_query = {
        "startDate": start_date.strftime("%Y%m%d"),
        "endDate": today.strftime("%Y%m%d"),
        "code": "",
        "cate": "",
        "type": "all",
        "order": "date",
        "id": "",
        "response": "json",
    }
    
    TSE_res = requests.get(TSE_URL, params=TSE_query)
    if TSE_res.status_code != 200:
        logger.error("Failed to fetch data from TWSE")
        raise Exception("Failed to fetch data from TWSE")
    else:
        logger.info("Data fetched successfully from TWSE")

    with open("TSE_notice.json", "w", encoding="utf-8") as f:
        json.dump(TSE_res.json(), f, ensure_ascii=False, indent=4)

    OTC_res = requests.get(OTC_URL, params=OTC_query)
    if OTC_res.status_code != 200:
        logger.error("Failed to fetch data from OTC")
        raise Exception("Failed to fetch data from OTC")
    else:
        logger.info("Data fetched successfully from OTC")

    with open("OTC_notice.json", "w", encoding="utf-8") as f:
        json.dump(OTC_res.json(), f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    fetch_data()
