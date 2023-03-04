import pymysql
import json

with open('config/config.json') as f:
    config = json.load(f)


def connect_db():
    conn = pymysql.connect(
        host=config['MYSQL_HOST'],
        user=config['MYSQL_USER'],
        password=config['MYSQL_PASSWORD'],
        db=config['MYSQL_DB'],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )
    return conn


def execute_query(conn, query, *args):
    with conn.cursor() as cursor:
        cursor.execute(query, args)
        result = cursor.fetchall()
    return result


def execute_insert_query(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(query)
    conn.commit()


def drop_tables_stock_trading(conn):
    drop_table_query = """
    DROP TABLE IF EXISTS stock_trading;
    """
    execute_query(conn, drop_table_query)
    print(f"stock_trading table is DROPPED!")


def drop_tables_stock_news(conn):
    drop_table_query = """
    DROP TABLE IF EXISTS stock_news;
    """
    execute_query(conn, drop_table_query)
    print(f"stock_news table is DROPPED!")


def drop_tables_stock_issues(conn):
    drop_table_query = """
    DROP TABLE IF EXISTS stock_issues;
    """
    execute_query(conn, drop_table_query)
    print(f"stock_issues table is DROPPED!")


def create_table_stock_trading(conn):
    create_table_query = """
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
    """
    execute_query(conn, create_table_query)


def create_table_stock_news(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_news (
        code VARCHAR(20) NOT NULL,
        article_date date NOT NULL,
        title VARCHAR(100) NOT NULL,
        stock_name varchar(100) NOT NULL,
        link VARCHAR(200) NOT NULL,
        PRIMARY KEY (code, article_date, title)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    execute_query(conn, create_table_query)


def create_table_stock_issues(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS stock_issues (
        report_time DATETIME NOT NULL,
        title VARCHAR(255) NOT NULL,
        stock_name VARCHAR(255) NOT NULL,
        news_link VARCHAR(255),
        news_content TEXT,
        PRIMARY KEY (report_time, title, stock_name)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    execute_query(conn, create_table_query)
