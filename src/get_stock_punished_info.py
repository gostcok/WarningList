import requests
import pandas as pd

from datetime import datetime, timedelta
import time
import json

from loguru import logger

def fetch_data(URL,query,period=90) -> pd.DataFrame:
    TSE_URL = "https://www.twse.com.tw/rwd/zh/announcement/punish"
    OTC_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/attention"

    today = datetime.now()
    start_date = (today - timedelta(days=period))
    
    if 'twse' in URL:
        query["startDate"] = today.strftime("%Y%m%d")
        query["endDate"] = today.strftime("%Y%m%d")
    else:
        query["startDate"] = start_date.strftime("%Y/%m/%d")
        query["endDate"] = today.strftime("%Y/%m/%d")

    res = requests.get(URL, params=query)
    
    if 'twse' in URL:
        if res.status_code != 200:
            logger.error("Failed to fetch data from TWSE")
            raise Exception("Failed to fetch data from TWSE")
        else:
            logger.info("Data fetched successfully from TWSE")
            with open("TSE_punished.json", "w", encoding="utf-8") as f:
                json.dump(res.json(), f, ensure_ascii=False, indent=4)
    else:
        if res.status_code != 200:
            logger.error("Failed to fetch data from OTC")
            raise Exception("Failed to fetch data from OTC")
        else:
            logger.info("Data fetched successfully from OTC")
            with open("OTC_punished.json", "w", encoding="utf-8") as f:
                json.dump(res.json(), f, ensure_ascii=False, indent=4)
                
if __name__ == "__main__":
    TSE_URL = "https://www.twse.com.tw/rwd/zh/announcement/punish"
    OTC_URL = "https://www.tpex.org.tw/www/zh-tw/bulletin/disposal"

    TSE_query = {
        "startDate": "",
        "endDate": "",
        "querytype": 3,
        "stockNo" : "",
        "selectType" : "",
        "proceType" : "",
        "remarkType" : "",
        "sortKind": "STKNO",
        "response": "json",
        "_": time.time() * 1000
    }
    OTC_query = {
        "startDate": "",
        "endDate": "",
        "code": "",
        "cate": "",
        "type": "all",
        "reason": -1,
        "measure": -1,
        "order": "code",
        "id": "",
        "response": "json",
    }

    for URL, query in [(TSE_URL, TSE_query) ,(OTC_URL, OTC_query)]:
        fetch_data(URL, query)
