import re
import asyncio
import json

import pytz
from telethon import TelegramClient
from datetime import datetime, timedelta
from telethon.tl.types import InputMessagesFilterEmpty

import db_handle

# 사용자 API ID 및 해시
with open('config/config.json') as f:
    config = json.load(f)

api_id = config['api_id']
api_hash = config['api_hash']
session_name = config['session_name']

# 텔레그램 클라이언트 인스턴스 생성
client = TelegramClient(session_name, api_id, api_hash)

# 검색기간
kst_timezone = pytz.timezone('Asia/Seoul')
end_date = datetime.now()
start_date = end_date - timedelta(days=5)
message_count = 60


async def get_messages(channel_name):
    # 채널 정보 가져오기
    channel_entity = await client.get_entity(channel_name)
    channel_id = channel_entity.id

    messages = []
    # messages = await client.get_messages(channel_id, limit=message_count)
    async for message in client.iter_messages(channel_id):
        kst_datetime = datetime.fromtimestamp(message.date.timestamp(), tz=kst_timezone)
        kst_time = kst_datetime.strftime("%Y-%m-%d %H:%M:%S")
        if kst_datetime.timestamp() > start_date.timestamp():
            messages.append((kst_time, message.text))
    return messages


async def bot_main(conn, channel_name):
    await client.start()
    messages = await get_messages(channel_name)
    message_list = []
    for date, message in messages:
        title, name, contents, link = extract_stock_issue(message)
        print(f'날짜 : {date} , 타이틀 : {title}, 종목명 : {name}, 이슈내용 : {contents}, 링크 : {link}')
        if title and name:
            title_str = '/'.join(title).replace("'", "''")
            name_str = ','.join(name).replace("'", "''")
            contents = contents.replace("'", "''")
            link_str = ','.join(link)
            message_list.append((date, title_str, name_str, contents, link_str))
        # print(f'날짜 : {date} , 내용 : {message}')

    if message_list:
        print(f'news_list : {message_list}')
        values = ','.join(map(lambda x: f"('{x[0]}', '{x[1]}', '{x[2]}', '{x[3]}', '{x[4]}')", message_list))
        sql = f"INSERT INTO stock_issues (report_time, title, stock_name, news_link, news_content) VALUES {values}"
        print(f'insert query {sql}')
        db_handle.execute_insert_query(conn, sql)
    await client.disconnect()


def extract_stock_issue(message):
    """
    주어진 문자열에서 종목명과 이슈내용 패턴을 추출하는 함수
    :param message: 주어진 문자열
    :return: 추출된 종목명과 이슈내용
    """

    """

    match_stock = re.search(r'\*\*\[(.*?)\]\*\*', text)
    if match_stock:
        stock_name = match_stock.group(1)
    else:
        stock_name = "종목명 없음"

    # 이슈내용 추출
    match_issue = re.search(r'([가-힣\s\w\.\,\(\)]+)[\[|\n]', text)
    if match_issue:
        issue_content = match_issue.group(1)
    else:
        issue_content = "이슈내용 없음"
    """
    # 이슈 타이틀 추출
    try:
        issue_title = re.findall(r"✅(.+)", message)
    except TypeError:
        issue_title = []

    # 종목명 추출
    try:
        # stock_name = re.findall(r"\[(.+)\]", message)
        # stock_name = re.findall(r"\[(?!\d).*?\]", message)
        # stock_name = [re.search(r"\[(?!\d)(.+?)\]", s).group(1) for s in re.findall(r"\[(?!\d).*?\]", message)]
        stock_name = list(
            set([re.search(r"\[(?!\d)(.+?)\]", s).group(1) for s in re.findall(r"\[(?!\d).*?\]", message)]))
    except TypeError:
        stock_name = []

    # 링크 추출
    try:
        link = re.findall(r"(https?://\S+)", message)
    except TypeError:
        link = []

    # 이슈 내용 추출
    try:
        issue_content = re.sub(r"✅(.+)", "", message)
        issue_content = re.sub(r"\[(?!\d)(.+?)\]", "", issue_content)
        issue_content = re.sub(r"(https?://\S+)", "", issue_content)
        issue_content = re.sub(r"\*\*", "", issue_content)  # ** 제거
        issue_content = re.sub(r"\n+", "/", issue_content).lstrip("/").rstrip("/")  # 뉴라인을 /로 변경
        issue_content = issue_content.strip()
    except TypeError:
        issue_content = ""

    # 타이틀,종목명,내용,링크
    return issue_title, stock_name, issue_content, link


if __name__ == '__main__':
    confirmation = input(f"DROP the tables 'stock_issues'? (y/n): ")
    # MySQL 연결 설정
    conn = db_handle.connect_db()

    # 테이블 삭제 유무
    if confirmation.lower() == 'y':
        db_handle.drop_tables_stock_issues(conn)
    else:
        print(f"stock_issues will be used.")

    # 테이블 생성
    db_handle.create_table_stock_issues(conn)

    channel = config['telegram_channel']
    asyncio.run(bot_main(conn, channel))
