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

# 뉴스 크롤링 함수
def crawl_news(code):
    # pykrx 라이브러리를 이용해서 종목 코드에 해당하는 종목명 조회
    name = stock.get_market_ticker_name(code)

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

    for page in range(1, max_page + 1):
        url = f'https://finance.naver.com/item/news_news.nhn?code={code}&page={page}&sm=title_entity_id.basic&clusterId='
        req = requests.get(url)
        soup = BeautifulSoup(req.text, 'html.parser')
        articles = soup.select('.type5 tbody tr')

        news_list = []
        for article in articles:
            # print(article)
            article_date = article.select_one('td.date').text
            article_date = datetime.strptime(article_date,' %Y.%m.%d %H:%M').date()
            title = article.select_one('td.title a').text.replace("'", "''").replace('"', "''") #작은따옴표 두 개로 변경
            link = article.select_one('td.title a')['href']

            # print(f'news {news}')
            # Check for duplicates
            # sql = f"SELECT count(*) FROM stock_news WHERE code='{code}' AND article_date='{article_date}' AND title = '{title}'"
            # print(f'sql : {sql}')
            # cursor.execute(sql)
            # result = cursor.fetchone()

            # dup check
            if (code,article_date,title) not in [(row[0], row[1], row[2]) for row in news_list]:
                # 중복된 뉴스가 없으면 뉴스 데이터를 추가
                news_list.append((code,article_date,title,name,link))
                print(f'news_list : {news_list}')

        if news_list:
            values = ','.join(map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}')", news_list))
            sql = f"INSERT IGNORE INTO stock_news (code, article_date, title, stock_name, link) VALUES {values}"
            print(f'insert query {sql}')
            cursor.execute(sql)
            conn.commit()

print(f'start_date : {start_date} , end_date : {end_date}')
drop_table_query1 = f"DROP TABLE stock_trading"
drop_table_query2 = f"DROP TABLE stock_news"

confirmation = input(f"stock_trading, stock_news 테이블을 DROP 하시겠습니까? (y/n): ")

if confirmation.lower() == 'y':
    with conn.cursor() as cursor:
        cursor.execute(drop_table_query1)
        cursor.execute(drop_table_query2)
    print(f"stock_trading, stock_news 테이블이 DROP 되었습니다.")
else:
    print(f"stock_trading, stock_news 테이블 재사용합니다.")

# 거래대금 테이블 생성
create_table_query1 = '''
CREATE TABLE IF NOT EXISTS stock_trading (
    code varchar(20) NOT NULL,
    trading_date date NOT NULL,
    stock_name varchar(100) NOT NULL,
    foreign_total bigint DEFAULT NULL,
    institution bigint DEFAULT NULL,
    individual bigint DEFAULT NULL,
    agency_total bigint DEFAULT NULL,
    PRIMARY KEY (code,trading_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
'''

# 뉴스 테이블 생성
create_table_query2 = '''
CREATE TABLE IF NOT EXISTS stock_news (
    code VARCHAR(20) NOT NULL,
    article_date date NOT NULL,
    title VARCHAR(100) NOT NULL,
    stock_name varchar(100) NOT NULL,
    link VARCHAR(200) NOT NULL,
    PRIMARY KEY (code, article_date, title)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
'''

with conn.cursor() as cursor:
    cursor.execute(create_table_query1)
    cursor.execute(create_table_query2)

kospi_codes = []
kosdaq_codes = []

while start_date <= end_date:
    # 일별 시세 조회
    kospi_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSPI')
    kosdaq_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSDAQ')

    # 거래대금이 500억 이상인 종목 필터링
    kospi_codes += [(start_date.strftime('%Y%m%d'), code) for code in kospi_ohlcv.loc[kospi_ohlcv['거래대금'] >= 50000000000].index.tolist()]
    kosdaq_codes += [(start_date.strftime('%Y%m%d'), code) for code in kosdaq_ohlcv.loc[kosdaq_ohlcv['거래대금'] >= 50000000000].index.tolist()]
    # 다음날로 이동
    start_date += timedelta(days=1)

codes = kospi_codes + kosdaq_codes
print(codes)

# 각 종목별 뉴스, 뉴스링크, 투자자 매매동향 스크래핑
news_code = []
try:
    with conn.cursor() as cursor:
        for dt, code in codes:
            name = stock.get_market_ticker_name(code)
            # 매매주체별 거래대금
            df2 = stock.get_market_trading_value_by_date(dt, dt, code)
            df2.insert(0, 'code', code)
            df2.reset_index(inplace=True)
            trade_list = []
            for i, row in df2.iterrows():
                trading_date = row['날짜'].strftime('%Y%m%d')
                # dup check
                if (code, trading_date) not in trade_list:
                    # print(f'trade_code : {trade_code}')
                    trade_list.append((row['code'], trading_date, name, row['외국인합계'], row['기관합계'], row['개인'], row['전체']))
            if trade_list:
                values = ','.join(map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}' , '{x[5]}' , '{x[6]}')", trade_list))
                sql = f"insert into stock_trading (code, trading_date, stock_name, foreign_total, institution, individual," \
                      f" agency_total) values  {values}"
                print(f'insert query : {sql}')
                cursor.execute(sql)
                conn.commit()

            if code not in news_code:
                news_code.append(code)
                crawl_news(code)

except Exception as e:
    print(str(e))
    traceback.print_exc()
finally:
    # Connection 닫기
    conn.close()
