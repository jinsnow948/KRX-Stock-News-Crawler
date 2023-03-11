import logging
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup
from pykrx import stock

import db_handle


def insert_stock_trading_data(conn, codes):
    news_code = []
    trade_list = []
    for dt, code in codes:
        name = stock.get_market_ticker_name(code)

        # 매매주체별 거래대금
        try:
            df2 = stock.get_market_trading_value_by_date(dt, dt, code)
            df2.insert(0, 'code', code)
            df2.reset_index(inplace=True)
        except Exception as e:
            logging.error(f"get_market_trading_value_by_date error {e} dt {dt}, code {code}");
            return []

        for i, row in df2.iterrows():
            trading_date = row['날짜'].strftime('%Y%m%d')
            # dup check
            if (code, trading_date) not in trade_list:
                # print(f'trade_code : {trade_code}')
                trade_list.append(
                    (row['code'], trading_date, name, row['외국인합계'], row['기관합계'], row['개인'], row['전체']))

        if code not in news_code:
            print(f'append news code : {code}')
            news_code.append(code)

    if trade_list:
        values = ','.join(
            map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}' , '{x[5]}' , '{x[6]}')",
                trade_list))
        sql = f"insert into stock_trading (code, trading_date, stock_name, foreign_total, institution, individual," \
              f" agency_total) values  {values}"
        print(f'insert query : {sql}')
        db_handle.execute_insert_query(conn, sql)

    for code in news_code:
        crawl_news(conn, code)


# 뉴스 크롤링 함수
def crawl_news(conn, code):
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
            article_date = datetime.strptime(article_date, ' %Y.%m.%d %H:%M').date()
            title = article.select_one('td.title a').text.replace("'", "''").replace('"', "''").replace('’', "''") \
                .replace('‘', "''").replace('“', "''").replace('”', "''")  # 작은따옴표 두 개로 변경
            link = article.select_one('td.title a')['href']

            # print(f'news {news}')
            # Check for duplicates
            # sql = f"SELECT count(*) FROM stock_news WHERE code='{code}' AND article_date='{article_date}' AND title = '{title}'"
            # print(f'sql : {sql}')
            # cursor.execute(sql)
            # result = cursor.fetchone()

            # dup check
            if (code, article_date, title) not in [(row[0], row[1], row[2]) for row in news_list]:
                # 중복된 뉴스가 없으면 뉴스 데이터를 추가
                news_list.append((code, article_date, title, name, link))

        if news_list:
            print(f'news_list : {news_list}')
            values = ','.join(map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}')", news_list))
            sql = f"INSERT IGNORE INTO stock_news (code, article_date, title, stock_name, link) VALUES {values}"
            print(f'insert query {sql}')
            db_handle.execute_insert_query(conn, sql)


def scrap_stock_data(start_date, end_date):
    """
    :param start_date: 시작일
    :param end_date: 종료일
    :return:
    """
    kospi_codes = []
    kosdaq_codes = []

    while start_date <= end_date:
        # 일별 시세 조회
        try:
            kospi_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSPI')
        except Exception as e:
            logging.error(f"Failed to fetch KOSPI OHLCV: {e}, start_date {start_date.strftime('%Y%m%d')}")
            return []
        try:
            kosdaq_ohlcv = stock.get_market_ohlcv(start_date.strftime('%Y%m%d'), market='KOSDAQ')
        except Exception as e:
            logging.error(f"Failed to fetch KOSDAQ OHLCV: {e}, start_date {start_date.strftime('%Y%m%d')}")
            return []

        # 거래대금이 500억 이상인 종목 필터링
        kospi_codes += [(start_date.strftime('%Y%m%d'), code) for code in
                        kospi_ohlcv.loc[kospi_ohlcv['거래대금'] >= 50000000000].index.tolist()]
        kosdaq_codes += [(start_date.strftime('%Y%m%d'), code) for code in
                         kosdaq_ohlcv.loc[kosdaq_ohlcv['거래대금'] >= 50000000000].index.tolist()]
        # 다음날로 이동
        start_date += timedelta(days=1)

    codes = kospi_codes + kosdaq_codes
    print(codes)

    return codes
