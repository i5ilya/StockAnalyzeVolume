from connection_to_db import connect
import datetime
from update_all_data_db import normalise_lists_from_db
from func_exec_and_read_querys_db import read_query_all
from typing import *

# connect to db
conn = connect()

# get data 6 month ago
date_6m_ago = datetime.date.today() - datetime.timedelta(days=180)

# get list of sp500 tables from db
sp500list = normalise_lists_from_db(read_query_all(conn, 'SELECT symbol FROM sp500_list'))


def find_key_by_value_in_dic(some_dict: Dict, value):
    return list(some_dict.keys())[list(some_dict.values()).index(value)]


def get_data(from_date, ticket: str):
    return read_query_all(conn, f"""select date, volume from "{ticket}" where date > '{from_date}'""")


def make_a_dic_simple(data_from_db) -> Dict:
    dic_simple = {}
    for items in data_from_db:
        # dataset_dic[str(items.date.date())] = items.volume, items.open, items.high, items.low, items.close
        dic_simple[str(items.date.date())] = items.volume
    return dic_simple


def list_of_values_from_dic(some_dic: Dict) -> List:
    return [item for item in some_dic.values()]


def average_value(some_list_of_value: List):
    return int(sum(some_list_of_value) / len(some_list_of_value))


if __name__ == '__main__':
    for ticket in sp500list:
        dataset_dic = make_a_dic_simple(get_data(date_6m_ago, ticket))
        volumes = list_of_values_from_dic(dataset_dic)
        average_volume = average_value(volumes)
        for value in volumes[-20:-1]:
            if value >= average_volume * 3:
                print(ticket.upper(), find_key_by_value_in_dic(dataset_dic, value))
