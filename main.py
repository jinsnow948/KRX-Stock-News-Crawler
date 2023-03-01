import pymysql
from pykrx import stock
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import traceback

# MySQL 연결 정보
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root123'
MYSQL_DB = 'mydb'

# MySQL 연결 설정
conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB, charset='utf8mb4')

# cursor 생성
cursor = conn.cursor()


# 뉴스 크롤링 함수
def crawl_news(code):
    url = f'https://finance.naver.com/item/news_news.nhn?code={code}&page=1&sm=title_entity_id.basic&clusterId='
    req = requests.get(url)
    soup = BeautifulSoup(req.text, 'html.parser')
    pages = soup.select('.Nnavi td a')
    max_page = 0
    for page in pages:
        try:
            page_num = int(page.text)
            if page_num > max_page:
                max_page = page_num
        except ValueError:
            continue

    news = []
    for page in range(1, max_page + 1):
        url = f'https://finance.naver.com/item/news_news.nhn?code={code}&page={page}&sm=title_entity_id.basic&clusterId='
        req = requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        articles = soup.select('.type5 tbody tr')
        for article in articles:
            print(article)
            article_date = article.select_one('td.date').text
            title = article.select_one('td.title a').text
            title = title.replace("'", "''")  # 작은따옴표를 두 개로 변경
            link = article.select_one('td.title a')['href']

            # dup check
            if (code, article_date, title) not in news:
                news.append((code, article_date, title))
                sql = f"INSERT INTO stock_news (code, article_date, title, link) VALUES ('{code}', '{article_date}', '{title}', '{link}')"
                print(f'insert query {sql}')
                cursor.execute(sql)
    conn.commit()


# 오늘 날짜
now = datetime.now()

# 날짜 구하기
# start_date = now - datetime.timedelta(days=5)
# start_date_str = start_date.strftime('%Y%m%d')
# end_date_str = now.strftime('%Y%m%d')


end_date_str = "20230224"
end_date = datetime.strptime(end_date_str, '%Y%m%d')

start_date = end_date - timedelta(days=30)
start_date_str = start_date.strftime('%Y%m%d')

drop_table_query1 = f"DROP TABLE stock_trading"
drop_table_query2 = f"DROP TABLE stock_news"

confirmation = input(f"stock_trading, stock_news 테이블을 DROP 하시겠습니까? (y/n): ")

if confirmation.lower() == 'y':
    cursor.execute(drop_table_query1)
    cursor.execute(drop_table_query2)
    conn.commit()
    print(f"stock_trading, stock_news 테이블이 DROP 되었습니다.")
else:
    print(f"stock_trading, stock_news 테이블이 재 사용합니다.")

# 거래대금 테이블 생성
create_table_query = '''
CREATE TABLE IF NOT EXISTS stock_trading (
    code varchar(20) NOT NULL,
    trading_date varchar(20) NOT NULL,
    foreign_total bigint DEFAULT NULL,
    institution bigint DEFAULT NULL,
    individual bigint DEFAULT NULL,
    agency_total bigint DEFAULT NULL,
    PRIMARY KEY (code,trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
'''

cursor.execute(create_table_query)

# 뉴스 테이블 생성
create_table_query = '''
CREATE TABLE IF NOT EXISTS stock_news (
    code VARCHAR(20) NOT NULL,
    article_date VARCHAR(20) NOT NULL,
    title VARCHAR(100) NOT NULL,
    link VARCHAR(200) NOT NULL,
    PRIMARY KEY (code, article_date, title)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
'''

cursor.execute(create_table_query)

kospi_codes = []
kosdaq_codes = []

while start_date <= end_date:
    # 일별 시세 조회
    kospi_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSPI')
    kosdaq_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSDAQ')

    # 거래대금이 500억 이상인 종목 필터링
    kospi_codes += kospi_ohlcv.loc[kospi_ohlcv['거래대금'] >= 50000000000].index.tolist()
    kosdaq_codes += kospi_ohlcv.loc[kospi_ohlcv['거래대금'] >= 50000000000].index.tolist()
    # 다음날로 이동
    start_date += timedelta(days=1)

codes = kospi_codes + kosdaq_codes
# print(codes)
# 각 종목별 뉴스, 뉴스링크, 투자자 매매동향 스크래핑
news_list = []

try:
    for code in codes:
        # 매매주체별 거래대금
        df2 = stock.get_market_trading_value_by_date(start_date_str, end_date_str, code)
        df2.insert(0, 'code', code)
        df2.reset_index(inplace=True)
        for i, row in df2.iterrows():
            sql = """insert into stock_trading (code, trading_date, foreign_total, institution, individual, 
            agency_total) values (%s, %s, %s, %s, %s, %s)"""
            val = (row['code'], row['날짜'], row['외국인합계'], row['기관합계'], row['개인'], row['전체'])
            cursor.execute(sql, val)
        conn.commit()

        crawl_news(code)

except Exception as e:
    print(str(e))
    traceback.print_exc()
finally:
    # Connection 닫기
    conn.close()
