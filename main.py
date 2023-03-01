import pymysql
from pykrx import stock
from bs4 import BeautifulSoup
import requests
import datetime

# MySQL 연결 정보
MYSQL_HOST = 'localhost'
MYSQL_USER = 'root'
MYSQL_PASSWORD = 'root123'
MYSQL_DB = 'mydb'

# MySQL 연결 설정
conn = pymysql.connect(host=MYSQL_HOST, user=MYSQL_USER, password=MYSQL_PASSWORD, db=MYSQL_DB, charset='utf8mb4')

# 오늘 날짜
now = datetime.datetime.now()

# 날짜 구하기
start_date = now - datetime.timedelta(days=5)
start_date_str = start_date.strftime('%Y%m%d')
end_date_str = now.strftime('%Y%m%d')

start_date_str ="20230220"
end_date_str ="20230224"

# cursor 생성
cursor = conn.cursor()

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
    trading_date VARCHAR(20) NOT NULL,
    title VARCHAR(100) NOT NULL,
    link VARCHAR(200) NOT NULL,
    PRIMARY KEY (code, trading_date, title)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
'''

cursor.execute(create_table_query)

# 코스피, 코스닥 종목 코드
kospi_codes = stock.get_market_ticker_list(market="KOSPI")
kosdaq_codes = stock.get_market_ticker_list(market="KOSDAQ")
market_codes = kospi_codes + kosdaq_codes
# 500억 이상 거래된 종목만 추출
codes = []
for code in market_codes:
    try:
        df = stock.get_market_ohlcv_by_ticker(code, start_date_str, end_date_str)
        if df["거래대금"] >= 500000000000:
            codes.append(code)
    except:
        pass

# 각 종목별 뉴스, 뉴스링크, 투자자 매매동향 스크래핑
news_list = []
for code in codes:
    try:
        # 매매주체별 거래대금
        df2 = stock.get_market_trading_value_by_date(start_date_str, end_date_str, code)
        df2.insert(0, 'code', code)
        df2.reset_index(inplace=True)
        for i, row in df2.iterrows():
            sql = """insert into stock_trading (code, trading_date, foreign_total, institution, individual, agency_total) 
                     values (%s, %s, %s, %s, %s, %s)"""
            val = (row['code'], row['날짜'], row['외국인'], row['기관계'], row['개인'], row['기관합계'])
            cursor.execute(sql, val)
            conn.commit()

        url = f'https://finance.naver.com/item/news_news.nhn?code={code}&page=1&sm=title_entity_id.basic&clusterId='
        req = requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        pages = soup.select('.Nnavi td')
        max_page = int(pages[-1].text)
        for page in range(1, max_page + 1):
            url = f'https://finance.naver.com/item/news_news.nhn?code={code}&page={page}&sm=title_entity_id.basic&clusterId='
            req = requests.get(url)
            soup = BeautifulSoup(req.text, 'html.parser')
            articles = soup.select('.type5 tbody tr')
            for article in articles:
                article_date = article.select_one('td.date').text
                title = article.select_one('td.title a').text
                link = article.select_one('td.title a')['href']
                sql = f"INSERT INTO news (code, trading_date, title, link) VALUES ('{code}', '{article_date}', '{title}', '{link}')"
                cursor.execute(sql)
        conn.commit()

    except Exception as e:
        print(str(e))

# Connection 닫기
conn.close()