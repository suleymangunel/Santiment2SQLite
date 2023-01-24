from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, Integer
from datetime import datetime, timezone
import schedule
import time
import requests
import os

SQLALCHEMY_WARN_20 = 1
basedir = os.path.abspath(os.path.dirname(__file__))
engine = create_engine('sqlite:///' + os.path.join(basedir, 'currency.db'), future=True)
meta = MetaData()  # meta = MetaData(bind=engine) for create table


def get_table(table_name):
    table = Table(
        table_name,
        meta,
        Column("id", Integer, primary_key=True, autoincrement=True, nullable=False),
        Column("value", Float, primary_key=False),
        Column("date", String, primary_key=False),
        extend_existing=True,
        autoload_with=engine
    )
    return table


# meta.create_all() #for create table


def insert_value(_currency_name, _value, _date):
    with engine.connect() as connection:
        with connection.begin():
            result = get_table(_currency_name).insert().values(value=_value, date=_date)
            connection.execute(result)


def read_currency_values():
    query_bitcoin_ethereum = '{allProjects(selector: {baseProjects: {slugs: ["ethereum", "bitcoin"]}}) {' \
                             'slug\n' \
                             'aggregatedTimeseriesData(metric: "price_usd", from: "utc_now-10m", to: "utc_now", ' \
                             'aggregation: LAST)}}'

    api_url_snt = "https://api.santiment.net/graphql?query=" + query_bitcoin_ethereum
    response = requests.get(api_url_snt)
    if response.status_code != 200:
        raise Exception("Error occurred")
    data = response.json()
    value_of_bitcoin = \
        list(x["aggregatedTimeseriesData"] for x in data["data"]["allProjects"] if x["slug"] == "bitcoin")[0]
    value_of_ethereum = \
        list(x["aggregatedTimeseriesData"] for x in data["data"]["allProjects"] if x["slug"] == "ethereum")[0]
    _values = {"bitcoin": value_of_bitcoin, "ethereum": value_of_ethereum}
    return _values


def save_to_database(_values):
    insert_value("bitcoin", _values["bitcoin"], datetime.now(timezone.utc))
    insert_value("ethereum", _values["ethereum"], datetime.now(timezone.utc))


def print_result(func):
    def wrapper():
        values = func()
        print("Bitcoin: ${}\nEthereum: ${}\nValues successfully saved to database"
              .format(values["bitcoin"], values["ethereum"]))
    return wrapper


@print_result
def task():
    values = read_currency_values()
    save_to_database(values)
    return values


if __name__ == '__main__':
    schedule.every(5).seconds.do(task)
    while True:
        time_to_next_job = schedule.idle_seconds()
        if time_to_next_job > 1:
            time.sleep(time_to_next_job - 1)
        schedule.run_pending()
