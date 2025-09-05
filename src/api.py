from flask import Flask, jsonify , render_template, request
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from get_stock_notice_infos import fetch_data
import notice_info_to_df
import punished_info_to_df
import get_trading_date
from get_targetInfo_to_db import target_info_to_db

app = Flask(__name__)

isfetchTargetinfo = False

def query_database(query, args=(),db_path="notice_stocks.db", one=False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, args)
    rv = cur.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv

def get_last_n_trading_range(n, db_path="trading_date.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 查出最近 n 個交易日（最新在最後）
    cursor.execute(f"""
        SELECT date FROM trading_date
        WHERE date <= DATE('now')
        ORDER BY date DESC
        LIMIT {n}
    """)
    dates = [row[0] for row in cursor.fetchall()]
    conn.close()
    
    # 確保足夠筆數才有範圍
    if len(dates) < n:
        return None, None
    return min(dates), max(dates)  # start_day, last_day

def generate_search_notice_info(n=1):
    notice_numbers = list(range(1, n+1))
    conditions = []

    for n in notice_numbers:
        conditions.extend([
            f"`注意交易資訊` = '[{n}]'",
            f"`注意交易資訊` LIKE '[{n},%'",
            f"`注意交易資訊` LIKE '%, {n}]'",
            f"`注意交易資訊` LIKE '%, {n},%'"
        ])
    
    return "(" + " OR ".join(conditions) + ")"
@app.route("/", methods=["GET"])
def home():
    return render_template("index.html")

@app.route("/stocks", methods=["GET"])
def get_stocks():
    stocks = query_database("SELECT * FROM stocks")
    return jsonify([dict(row) for row in stocks])

@app.route("/stocks/<stock_id>", methods=["GET"])
def get_stock(stock_id):
    stock = query_database("SELECT * FROM stocks WHERE `證券代號` = ?", [stock_id], one=True)
    return jsonify(dict(stock)) if stock else (jsonify({"error": "Stock not found"}), 404)

def query_punished_stocks(Source="" , sort_by="end_date"):
    conn = sqlite3.connect("punished_stocks.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if sort_by == "code":
        sort_by_query = " ORDER BY `證券代號` ASC"
    elif sort_by == "end_date":
        sort_by_query = " ORDER BY `處置結束時間` ASC"
    cur.execute(f"""
                SELECT `證券代號`, `證券名稱`, `處置起始時間`, `處置結束時間` FROM stocks
                WHERE 1=1 
                {Source}
                {sort_by_query}
                """
            )
    punished_stocks = {row["證券代號"] : {'證券名稱' : row['證券名稱'],
                                        '處置起始時間' : row['處置起始時間'],
                                        '處置結束時間' : row['處置結束時間']} for row in cur.fetchall()}
    conn.close()
    return punished_stocks

@app.route("/potential_disposals", methods=["GET"])
def get_potential_disposals():
    # 取得來源參數
    source = request.args.get("source", "all")
    sort_by = request.args.get("sort_by", "code")

    # 拿出不同條件所需的交易日範圍
    start_day_2, last_day_2 = get_last_n_trading_range(2)
    start_day_4, last_day_4 = get_last_n_trading_range(4)
    start_day_9, last_day_9 = get_last_n_trading_range(9)
    start_day_29, last_day_29 = get_last_n_trading_range(29)

    search_notice_info_1 = generate_search_notice_info(1)
    search_notice_info_8 = generate_search_notice_info(8)
    if not (start_day_2 and start_day_4 and start_day_9 and start_day_29):
        return jsonify({"error": "無法取得足夠的交易日資料"}), 500

    # 根據 source 加入條件
    source_condition = ""
    if source == "TSE":
        source_condition = "AND `Source` = 'TSE'"
    elif source == "OTC":
        source_condition = "AND `Source` = 'OTC'"

    query = f"""
    SELECT DISTINCT `證券代號`, `證券名稱`, `注意交易資訊`, `累計次數`, `日期`
    FROM stocks
    WHERE (
        `證券代號` IN (
            SELECT `證券代號`
            FROM stocks
            WHERE {search_notice_info_1}
            AND `日期` BETWEEN DATE('{start_day_2}') AND DATE('{last_day_2}')
            {source_condition}
            GROUP BY `證券代號`
            HAVING COUNT(DISTINCT `日期`) = 2
        )
        AND `日期` BETWEEN DATE('{start_day_2}') AND DATE('{last_day_2}')
        {source_condition}
    ) OR (
        `證券代號` IN (
            SELECT `證券代號`
            FROM stocks
            WHERE {search_notice_info_8}
            AND `日期` BETWEEN DATE('{start_day_4}') AND DATE('{last_day_4}')
            {source_condition}
            GROUP BY `證券代號`
            HAVING COUNT(DISTINCT `日期`) >= 4
        )
        AND `日期` BETWEEN DATE('{start_day_4}') AND DATE('{last_day_4}')
        {source_condition}
    ) OR (
        `證券代號` IN (
            SELECT `證券代號`
            FROM stocks
            WHERE {search_notice_info_8}
            AND `日期` BETWEEN DATE('{start_day_9}') AND DATE('{last_day_9}')
            {source_condition}
            GROUP BY `證券代號`
            HAVING COUNT(DISTINCT `日期`) >= 5
        )
        AND `日期` BETWEEN DATE('{start_day_9}') AND DATE('{last_day_9}')
        {source_condition}
    ) OR (
        `證券代號` IN (
            SELECT `證券代號`
            FROM stocks
            WHERE {search_notice_info_8}
            AND `日期` BETWEEN DATE('{start_day_29}') AND DATE('{last_day_29}')
            {source_condition}
            GROUP BY `證券代號`
            HAVING COUNT(DISTINCT `日期`) >= 11
        )
        AND `日期` BETWEEN DATE('{start_day_29}') AND DATE('{last_day_29}')
        {source_condition}
    )
    """
    notice_stocks = query_database(query)

    # 查詢已處置股票
    punished_stocks = query_punished_stocks(source_condition , sort_by = sort_by)

    # 轉 dict，方便查詢
    notice_dict = {s["證券代號"]: {"證券名稱" : s["證券名稱"]} for s in notice_stocks}

    # 合併所有股票代號
    all_stock = (notice_dict) | (punished_stocks)
    
    # Extract unique stock names and codes
    unique_stocks = {}
    global isfetchTargetinfo
    for stock_code in all_stock:
        stock_name = all_stock[stock_code]['證券名稱']
        is_punished = stock_code in punished_stocks
        unique_stocks[stock_code] = {
            "證券名稱": stock_name,
            "已處置": is_punished,
            "處置起始時間": punished_stocks[stock_code]['處置起始時間'] if is_punished else "",
            "處置結束時間": punished_stocks[stock_code]['處置結束時間'] if is_punished else ""
        }
        if not isfetchTargetinfo : 
            target_info_to_db(stock_code)
    isfetchTargetinfo = True
    
    # 轉成 list 後依 sort_by 排序
    result = [
        {
            "證券代號": code,
            "證券名稱": info["證券名稱"],
            "已處置": info["已處置"],
            "處置起始時間": info["處置起始時間"],
            "處置結束時間": info["處置結束時間"],
        }
        for code, info in unique_stocks.items()
    ]

    if sort_by == "code":
        result.sort(key=lambda x: x["證券代號"])
    else:  # end_date
        # 將空值放最後，日期用 ISO 字串可直接比較；若不是 ISO，換成 datetime.strptime
        def end_key(x):
            v = x["處置結束時間"]
            return ("~" if not v else "0") + (v or "")  # "~" 比任何數字/字母大，放最後
        result.sort(key=end_key)
    # Return only unique stock names and codes
    return jsonify(result)

@app.route("/disposed_stocks", methods=["GET"])
def get_disposed_stocks():
    sort_by = request.args.get("sort_by", "code")

    conn = sqlite3.connect("punished_stocks.db")
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    query = "SELECT `證券代號`, `處置結束時間` FROM stocks"

    if sort_by == "code":
        query += " ORDER BY `證券代號` ASC"
    elif sort_by == "end_date":
        query += " ORDER BY `處置結束時間` ASC"

    cur.execute(query)
    stocks = [
        {"證券代號": row["證券代號"], "處置結束時間": row["處置結束時間"]}
        for row in cur.fetchall()
    ]
    conn.close()
    return jsonify(stocks)

@app.route("/stocks/<stock_id>/conditions", methods=["GET"])
def get_stock_conditions(stock_id):
    # 獲取最近的交易日範圍
    start_day_2, last_day_2 = get_last_n_trading_range(2)
    start_day_4, last_day_4 = get_last_n_trading_range(4)
    start_day_9, last_day_9 = get_last_n_trading_range(9)
    start_day_29, last_day_29 = get_last_n_trading_range(29)

    if not (start_day_2 and start_day_4 and start_day_9 and start_day_29):
        return jsonify({"error": "無法取得足夠的交易日資料"}), 500

    # 定義條件
    conditions = []

    # 條件 1: 連續 2 日符合第一款條件
    query_1 = f"""
        SELECT COUNT(DISTINCT `日期`) as count
        FROM stocks
        WHERE `證券代號` = ?
        AND {generate_search_notice_info(1)}
        AND `日期` BETWEEN DATE('{start_day_2}') AND DATE('{last_day_2}')
    """
    result_1 = query_database(query_1, [stock_id], one=True)
    if result_1 and result_1["count"] == 2:
        conditions.append("連續 2 日符合第一款條件")

    # 條件 2: 連續 4 日符合第 1~8 款條件
    query_2 = f"""
        SELECT COUNT(DISTINCT `日期`) as count
        FROM stocks
        WHERE `證券代號` = ?
        AND {generate_search_notice_info(8)}
        AND `日期` BETWEEN DATE('{start_day_4}') AND DATE('{last_day_4}')
    """
    result_2 = query_database(query_2, [stock_id], one=True)
    if result_2 and result_2["count"] >= 4:
        conditions.append("連續 4 日符合第 1~8 款條件")

    # 條件 3: 9 日內有 5 日符合第 1~8 款條件
    query_3 = f"""
        SELECT COUNT(DISTINCT `日期`) as count
        FROM stocks
        WHERE `證券代號` = ?
        AND {generate_search_notice_info(8)}
        AND `日期` BETWEEN DATE('{start_day_9}') AND DATE('{last_day_9}')
    """
    result_3 = query_database(query_3, [stock_id], one=True)
    if result_3 and result_3["count"] >= 5:
        conditions.append("9 日內有 5 日符合第 1~8 款條件")

    # 條件 4: 29 日內有 11 日符合第 1~8 款條件
    query_4 = f"""
        SELECT COUNT(DISTINCT `日期`) as count
        FROM stocks
        WHERE `證券代號` = ?
        AND {generate_search_notice_info(8)}
        AND `日期` BETWEEN DATE('{start_day_29}') AND DATE('{last_day_29}')
    """
    result_4 = query_database(query_4, [stock_id], one=True)
    if result_4 and result_4["count"] >= 11:
        conditions.append("29 日內有 11 日符合第 1~8 款條件")

    return jsonify(conditions)

@app.route("/stocks/<stock_id>/targetInfo", methods=["GET"])
def get_targetInfo(stock_id):

    start_day_2, last_day_2 = get_last_n_trading_range(2)
    start_day_4, last_day_4 = get_last_n_trading_range(4)
    start_day_9, last_day_9 = get_last_n_trading_range(9)
    start_day_29, last_day_29 = get_last_n_trading_range(29)
    # 定義條件
    lt=[]
    # 條件 1: 連續 2 日符合第一款條件
    query_1 = f"""
        SELECT `target_info1_1(%)`,`target_info1_2(%+N)`,`target_info2(%)`,`target_info2($)`,`target_info3(%)`,`target_info3(volume)`
        FROM target_info
        WHERE `code` = {stock_id}
        AND `ts` BETWEEN DATE('{start_day_29}') AND DATE('{last_day_29}')
    """
    targetInfo = query_database(query_1,db_path="target_info.db", one=False)
    
    # 轉換為陣列格式
    data = {
        "target_info_1": [],
        "target_info_2": [],
        "target_info_3": [],
        "target_info_4": [],
        "target_info_5": [],
        "target_info_6": [],
        "target_info_7": [],
        "target_info_8": [],
    }
    for row in targetInfo:
        if row["target_info1_1(%)"]:
            data["target_info_1"].append(row["target_info1_1(%)"])
        if row["target_info1_2(%+N)"]:
            data["target_info_1"].append(row["target_info1_2(%+N)"])
            
        if row["target_info2(%)"]:
            data["target_info_2"].append(row["target_info2(%)"])
        if row["target_info2($)"]:
            data["target_info_2"].append(row["target_info2($)"])
            
        if row["target_info3(%)"]:
            data["target_info_3"].append(row["target_info3(%)"])
        if row["target_info3(volume)"]:
            data["target_info_3"].append(row["target_info3(volume)"])
    
    return jsonify(data)


def update_data():
    fetch_data()
    notice_info_to_df.save_to_database()
    get_trading_date.save_to_database()
    print(123)
    
scheduler = BackgroundScheduler()
scheduler.add_job(update_data, 'cron', hour=0, minute=0)
scheduler.start()

if __name__ == "__main__":
    app.run(debug=True)
