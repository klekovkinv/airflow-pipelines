from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import sqlite3

import pandas as pd

CONN = sqlite3.connect('example.db')
EMAIL = "your-email@yandex.ru"
PASSWORD = 'your-password'
HOST = "smtp.yandex.ru"


def extract_currency(date):
    """
    data : YYYY-MM-DD
    """
    url = f'https://api.exchangerate.host/timeseries?start_date={date}&end_date={date}&base=EUR&symbols=USD&format=csv'

    return pd.read_csv(url)


def extract_data(date):
    """
    data: YYYY-MM-DD
    """
    url = f'https://raw.githubusercontent.com/dm-novikov/stepik_airflow_course/main/data_new/{date}.csv'

    return pd.read_csv(url)


def insert_to_db(df, table_name, conn):
    return df.to_sql(name=table_name, con=conn, if_exists='replace')


def sql_query(sql, conn):
    cursor = conn.cursor()
    result = cursor.execute(sql).fetchall()
    conn.commit()

    return result


def html_pretty(df):
    """ Pretty html dataframe
    """
    return """\
    <html>
      <head></head>
      <body>
        {0}
      </body>
    </html>
    """.format(df.to_html())


def send_report(content, email, password, host, subject=''):
    """ Send DF to email
    """

    msg = MIMEMultipart()
    msg['Subject'] = subject
    part = MIMEText(html_pretty(content), 'html')
    msg.attach(part)

    server = smtplib.SMTP(host, 587)
    server.starttls()
    server.login(email, password)
    server.sendmail(email, email, msg.as_string())
    server.quit()


def main(date, email, conn):
    # extract data
    currency = extract_currency(date)
    data = extract_data(date)

    # create table
    create_joined_query = (
        'CREATE TABLE IF NOT EXISTS joined ('
        'date DATE,'
        'code VARCHAR(5),'
        'rate FLOAT(0),'
        'base VARCHAR(5),'
        'value INT)'
    )

    sql_query(create_joined_query, conn)

    # insert data to tables
    insert_to_db(currency, 'currency', conn)
    insert_to_db(data, 'data', conn)

    # join tables
    join_query = (
        'INSERT INTO joined (date, code, rate, base, value)'
        'SELECT c.date, c.code, c.rate, c.base, d.value '
        'FROM currency AS c '
        'JOIN data AS d '
        'ON c.date = d.date AND c.base = d.currency'
    )

    sql_query(join_query, conn)

    # get data from joined table
    report = pd.read_sql('select * from joined', conn)

    # send report to email
    email_subject = f'Report {date}'
    send_report(report, EMAIL, PASSWORD, HOST, email_subject)

    # remove data from tables
    currency_truncate_query = (
        'DELETE FROM currency'
    )
    data_truncate_query = (
        'DELETE FROM data'
    )
    joined_truncate_query = (
        'DELETE FROM joined'
    )

    sql_query(currency_truncate_query, conn)
    sql_query(data_truncate_query, conn)
    sql_query(joined_truncate_query, conn)


if __name__ == "__main__":
    dates_list = [f'2021-01-0{i}' for i in range(1, 5)]

    for date in dates_list:
        main(date, EMAIL, CONN)
