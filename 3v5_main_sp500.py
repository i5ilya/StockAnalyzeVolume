import pandas as pd
from connection_to_db import connect
import datetime
from func_exec_and_read_querys_db import read_query_all, read_query_one
from queries import query_get_list_of_tables
from typing import *
from price_and_vol_patterns import volume_price_level_calc
from update_all_data_db import normalise_lists_from_db, dl_data_to_db
import matplotlib.pyplot as plt
import mplfinance as mpf

# connect to db
conn = connect()

# get data 6 month ago
date_6m_ago = datetime.date.today() - datetime.timedelta(days=180)

# get list of sp500 tables from db
sp500list = normalise_lists_from_db(read_query_all(conn, 'SELECT symbol FROM sp500_list'))


def find_key_by_value_in_dic(some_dict: Dict, value):
    return list(some_dict.keys())[list(some_dict.values()).index(value)]


def get_data(from_date, ticket: str, tf):
    return read_query_all(conn, f"""select date, volume from "{ticket}_{tf}" where date > '{from_date}'""")


def make_a_dic_simple(data_from_db) -> Dict:
    dic_simple = {}
    for items in data_from_db:
        # dataset_dic[str(items.date.date())] = items.volume, items.open, items.high, items.low, items.close
        dic_simple[str(items.date.date())] = items.volume
    return dic_simple


def list_of_values_from_dic(some_dic: Dict) -> List:
    return [item for item in some_dic.values()]


def average_value(some_list_of_value: List) -> float:
    return int(sum(some_list_of_value) / len(some_list_of_value))


def make_dic_key_and_list_of_values(list_of_dictionaries: list) -> Dict:
    """
    Функция в списке словарей удаляет дубли ключей и их значения добавляет в список к ключам.
    :param list_of_dictionaries: список простых словарей, ключ и одно значение.
    :return: Словарь: ключ: список значений.
    """
    # сначала мы забираем все ключи в множество(автоматом удаляем дубли ключей):
    all_keys = {list(items)[0] for items in list_of_dictionaries}
    new_dic = {}
    for item in list_of_dictionaries:
        for key in all_keys:
            if item.get(key) is not None:
                if key not in new_dic:
                    # print(key, item[key] )
                    # new_dic[key].append(item[key])
                    new_dic[key] = item[key]
                else:
                    new_dic[key] += item[key]
    return new_dic


def convert_str_to_dt(date_as_str: str):
    """
    Function to convert string to datetime
    :param date_as_str: date as string
    :return: date as date, format 2022-05-19
    """
    format_date = '%Y-%m-%d'
    return datetime.datetime.strptime(date_as_str, format_date)


def check_and_dl_5m_data_yf(ticket: str, from_date, to_date, tf: str):
    """
    :param ticket:
    :param from_date:
    :param to_date:
    :param tf:
    :return:
    """
    symbols_in_db = normalise_lists_from_db(read_query_all(conn, query_get_list_of_tables))
    if ticket + '_' + tf in symbols_in_db:
        date_db = read_query_one(conn,
                                 f"""select date from "{ticket}_{tf}" where date BETWEEN '{from_date}' AND '{to_date}'""")
        if date_db is None:
            dl_data_to_db(ticket, symbols_in_db, from_date, to_date, tf='5m')
        else:
            print(f'Данные по {ticket}_{tf} да дату {from_date} есть в БД')
    else:
        dl_data_to_db(ticket, symbols_in_db, from_date, to_date, tf='5m')


def dl_5m_to_db_from_dic(some_dict):
    """
    :param some_dict: на вход принимает словарь с ключами - тикетами, значениями - список дат.
    :return: ничего не возвращает, скачивает и помещает в БД
    """
    for key, value in some_dict.items():
        for item in value:
            date_plus_23_h = convert_str_to_dt(item) + datetime.timedelta(
                hours=23)
            check_and_dl_5m_data_yf(key.lower(), convert_str_to_dt(item), date_plus_23_h, '5m')


def convert_data_from_db_to_pandas_df(raw_data_from_db):
    if raw_data_from_db is not None:
        data_df = pd.DataFrame(raw_data_from_db)
        data_df = data_df.sort_values(by='date')
        data_df = data_df.set_index('date')
        return data_df


def get_data_all_columns(ticket: str, tf: str, from_date, to_date=datetime.date.today()):
    data_from_db = read_query_all(conn,
                                  f"""select * from "{ticket}_{tf}" where date BETWEEN '{from_date}' AND '{to_date}'""")
    if data_from_db:
        return data_from_db
    else:
        print(f'The data in database by symbol {ticket} from date {from_date} to {to_date} is EMPTY!')


def make_dic_key_and_2_list_values(dic_key_and_list_values):
    """
    Создаем словарь: ключ плюс 2 кортежа списков значений (список дат и список уровней)
    :param dic_key_and_list_values: словарь с ключом и списком значений (дат)
    :return: словарь: ключ плюс 2 списка значений
    """
    dic_key_and_2_list_values = {}
    for key, value in dic_key_and_list_values.items():
        list_of_prices = []
        for item in value:
            date_plus_one_day = convert_str_to_dt(item) + datetime.timedelta(
                days=1)
            # забираем м5 данные из базы, конвертируем в pandas dataframe
            df = convert_data_from_db_to_pandas_df(
                get_data_all_columns(key.lower(), '5m', convert_str_to_dt(item), date_plus_one_day))
            if not df.empty:
                # и высчитываем уровень, добавляя в список уровней
                list_of_prices.extend(volume_price_level_calc(df))
            else:
                print(f'Data by {key}, {value} not in database')
        dic_key_and_2_list_values[key] = dic_key_and_list_values[key], list_of_prices
    return dic_key_and_2_list_values


def mplfinance_plotter(ticker, dataframe_d1, list_h_lines, list_v_lines):
    print(f'Creating fig {ticker}...')
    fig, axes = mpf.plot(dataframe_d1, type='candle', volume=True,
                         hlines=dict(hlines=list_h_lines, linewidths=0.5),
                         vlines=dict(vlines=list_v_lines, linewidths=0.5), returnfig=True)
    axes[0].set_title(ticker)
    print(f'Saving fig {ticker} on disk...')
    fig.savefig(f"img/{ticker}.png")
    plt.close(fig)


if __name__ == '__main__':
    list_of_dic = []
    for ticket in sp500list:
        dataset_dic = make_a_dic_simple(get_data(date_6m_ago, ticket, '1d'))
        volumes = list_of_values_from_dic(dataset_dic)
        average_volume = average_value(volumes)

        for value in volumes[-35:-1]:
            if value >= average_volume * 3:
                # print(ticket.upper(), find_key_by_value_in_dic(dataset_dic, value))
                list_of_dic.append({ticket.upper(): [find_key_by_value_in_dic(dataset_dic, value)]})

    # print(list_of_dic)
    # print(make_dic_key_and_list_of_values(list_of_dic))
    good_dict = make_dic_key_and_list_of_values(list_of_dic)
    print(good_dict)

    dl_5m_to_db_from_dic(good_dict)
    result = make_dic_key_and_2_list_values(good_dict)
    print(result)
    for key, value in result.items():
        # print(key, value[0])
        data = convert_data_from_db_to_pandas_df(get_data_all_columns(key.lower(), '1d', date_6m_ago))
        mplfinance_plotter(key, data, value[1], value[0])

    # check_and_dl_5m_data_yf('bll', '2022-05-13', '2022-05-14', '5m')

    # dataframe = pd.DataFrame(list(good_dict.items()), columns=['Key', 'H_Lines'])
    # dataframe = dataframe.set_index('Key')
    # print(dataframe)
    # print(dataframe.H_Lines[dataframe.index == 'DISH'].values[0])
    # print(good_dict['DISH'])

    # dic1 = {'DISH': ['2022-05-06', '2022-05-09', '2022-05-11', '2022-05-12'], 'DLTR': ['2022-05-18', '2022-05-26']}
    # for key, value in dic1.items():
    #     l = []
    #     for item in value:
    #         date_plus_one_date = convert_str_to_dt(item) + datetime.timedelta(
    #             days=1)
    #         df = convert_data_from_db_to_pandas_df(get_data_all_columns(key.lower(), '5m', convert_str_to_dt(item), date_plus_one_date))
    #         l.extend(volume_price_level_calc(df))
    #     dic1[key] = dic1[key], l
    # print(dic1['DISH'][1])
    # print(make_dic_key_and_2_list_values(dic1))
