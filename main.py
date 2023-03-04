import asyncio

from db_handle import *
import traceback

from crawl_data import *
from newsbot import bot_main

if __name__ == "__main__":
    with open('config/config.json') as f:
        config = json.load(f)

    telegram_channel = config['telegram_channel'],
    # MySQL 연결 설정
    conn = db_handle.connect_db()

    # cursor 생성
    # cursor = conn.cursor()

    # 날짜 구하기
    end_date = datetime.now()
    end_date_str = end_date.strftime('%Y%m%d')

    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime('%Y%m%d')
    '''
    end_date_str = "20230224"
    end_date = datetime.strptime(end_date_str, '%Y%m%d')

    start_date = end_date - timedelta(days=30)
    start_date_str = start_date.strftime('%Y%m%d')
    '''

    print(f'start_date : {start_date} , end_date : {end_date}')
    confirmation = input(f"DROP the tables 'stock_trading' and 'stock_news'? (y/n): ")

    # 테이블 삭제 유무
    if confirmation.lower() == 'y':
        db_handle.drop_tables_stock_news(conn)
        db_handle.drop_tables_stock_trading(conn)
    else:
        print(f"stock_trading, stock_news will be used.")

    # 테이블 생성
    db_handle.create_table_stock_trading(conn)
    db_handle.create_table_stock_news(conn)

    code_list = scrap_stock_data(start_date, end_date)
try:
    insert_stock_trading_data(conn, code_list)
except Exception as e:
    print(str(e))
    traceback.print_exc()
finally:
    # Connection 닫기
    conn.close()

try:
    asyncio.run(bot_main(telegram_channel))
except Exception as e:
    print(str(e))
    traceback.print_exc()
