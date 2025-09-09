import os
import shioaji as sj
from dotenv import load_dotenv

load_dotenv()

_API = None  # 模組級快取

def get_api(simulation: bool = True) -> sj.Shioaji:
    """懶加載：第一次呼叫才登入，之後重用同一個實例。"""
    global _API
    if _API is not None:
        return _API
    api = sj.Shioaji(simulation=simulation)
    api.login(
        api_key=os.environ["API_KEY"],
        secret_key=os.environ["SECRET_KEY"],
    )
    _API = api
    return _API

# 避免在 import 時就登入
if __name__ == "__main__":
    # 單獨執行 login.py 才會測試登入
    get_api()