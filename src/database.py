import sqlite3

def initialize_database():
    connection = sqlite3.connect("stocks.db")
    cursor = connection.cursor()

    # 建立 stocks 表格
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stock_code TEXT NOT NULL,
            stock_name TEXT NOT NULL,
            price_change REAL,
            volume INTEGER,
            attention_count INTEGER DEFAULT 0,
            is_disposed BOOLEAN DEFAULT 0
        )
    ''')

    connection.commit()
    connection.close()

if __name__ == "__main__":
    initialize_database()
