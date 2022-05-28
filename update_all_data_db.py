from connection_to_db import connect
from queries import query_get_list_of_tables
from func_exec_and_read_querys_db import read_query_all, execute_query, copy_from_stringio, read_query_one, \
    dl_data_yf_period, query_delete_duplicates
from queries import query_get_list_of_tables, query_create_table
import yfinance as yf
import pandas as pd
from datetime import datetime
import datetime
import time

conn = connect()


def normalise_lists_from_db(raw_list):
    return [item[0].lower() for item in raw_list]


# def dl_data_from_yahoo_to_db(symbols_to_dl, symbols_in_database):
#     for symbol in symbols_to_dl:
#         if symbol not in symbols_in_database:
#             print(f'Символа {symbol}, нет в базе данных, скачиваем...')
#             df = yf.download(symbol, period='6mo')
#             print(f'Создаем таблицу {symbol}...')
#             execute_query(conn, query_create_table(symbol))
#             print(f'Заполняем таблицу данными {symbol}...')
#             copy_from_stringio(conn, df, symbol)
#         else:
#             print(f'Символ {symbol} присутствует в базе, проверяем последнюю запись в базе...')
#             last_date_raw = read_query_one(conn, f'SELECT max(date) FROM "{symbol}"')
#             last_date = last_date_raw[0].date()
#             if last_date != datetime.date.today() and last_date != datetime.date.today() - datetime.timedelta(days=1):
#                 if last_date is not None:
#                     print(f'База данных {symbol} содержит последнюю запись от {last_date}')
#                     #  days_plus_one_from_db = last_date[0] + datetime.timedelta(days=1) # если грузим повторно в рабочий день, то день с заврашний)
#                     print(f'Скачиваем данные с {last_date} по {datetime.date.today()}... ')
#                     df_d = dl_data_yf_period(symbol, last_date, datetime.date.today())
#                     print(f'Загружаем полученные данные по {symbol} в БД...')
#                     copy_from_stringio(conn, df_d, symbol)
#                     print(f'Удаляем возможные дубли в базе {symbol}')
#                     execute_query(conn, query_delete_duplicates(symbol, 'date'))
#                 else:
#                     print(f'База данных {symbol} существует, но она пустая! Пробуем скачать данные еще раз...')
#                     df = yf.download(symbol, period='6mo')
#                     print(f'Заполняем таблицу данными {symbol}...')
#                     copy_from_stringio(conn, df, symbol)
#                     last_date = read_query_one(conn, f'SELECT max(date) FROM {symbol}')
#                     if last_date is not None:
#                         print(f'Кажется, база заполнилась)')
#                     else:
#                         print(f'Какие-то проблемы с загрузкой {symbol}')
#                         problem_with_data_load.append(symbol)
#             else:
#                 print(f'База данных {symbol} содержит последнюю запись от {last_date}')
#                 print(f'Данные {symbol} в БД актуальны и не требуют загрузки')

def dl_data_to_db(symbol, symbols_in_database, date_from, date_to, tf='1d'):
    """
    Функция создания таблицы и загрузки данных в БД из yahoofinance
    :param symbols_in_database: таблицы в БД
    :param symbol который хотим загрузить
    :param date_from: с какой даты
    :param date_to: по какую дату
    :param tf: таймфрейм
    :return: ничего не возвращает, а делает запись в БД, с созданием таблицы, если ее нет.
    """
    df = dl_data_yf_period(symbol, date_from, date_to, tf)
    if symbol + '_' + tf in symbols_in_database:
        print(f'Заполняем таблицу данными {symbol}_{tf}...')
        copy_from_stringio(conn, df, symbol + '_' + tf)
        print(f'Удаляем возможные дубли в базе {symbol}_{tf}')
        execute_query(conn, query_delete_duplicates(symbol + '_' + tf, 'date'))
    else:
        print(f'Создаем таблицу {symbol}_{tf}...')
        execute_query(conn, query_create_table(symbol + '_' + tf))
        print(f'Заполняем таблицу данными {symbol}_{tf}...')
        copy_from_stringio(conn, df, symbol + '_' + tf)
        print(f'Удаляем возможные дубли в базе {symbol}_{tf}')
        execute_query(conn, query_delete_duplicates(symbol + '_' + tf, 'date'))


def dl_and_update_d1_data(symbols_to_dl, symbols_in_database):
    tf = '1d'
    for symbol in symbols_to_dl:
        if symbol + '_' + tf not in symbols_in_database:
            print(f'Символа {symbol}_{tf}, нет в базе данных...')
            dl_data_to_db(symbol, symbols_in_database, datetime.date.today() - datetime.timedelta(days=180),
                          datetime.date.today())
        else:
            last_date_raw = read_query_one(conn, f'SELECT max(date) FROM "{symbol}_{tf}"')
            last_date = last_date_raw[0].date()
            if last_date is None:
                print(f'База данных {symbol}_{tf} существует, но она пустая! Пробуем скачать данные еще раз...')
                dl_data_to_db(symbol, symbols_in_database, datetime.date.today() - datetime.timedelta(days=180),
                              datetime.date.today())
            else:
                if last_date != datetime.date.today() and last_date != datetime.date.today() - datetime.timedelta(
                        days=1):
                    print(f'База данных {symbol}_{tf} содержит последнюю запись от {last_date}')
                    print(f'Скачиваем данные с {last_date} по {datetime.date.today()}... ')
                    dl_data_to_db(symbol, symbols_in_database, last_date, datetime.date.today())
                else:
                    print(f'База данных {symbol}_{tf} содержит последнюю запись от {last_date}')
                    print(f'Данные {symbol}_{tf} в БД актуальны и не требуют загрузки')


def check_for_empty_table(table_name):
    if table_name != "sp500_list":
        lastdate_in_table = read_query_one(conn, f'SELECT max(date) FROM "{table_name}"')
        if lastdate_in_table is not None:
            return False
        else:
            return True


if __name__ == '__main__':

    symbols_sp500 = normalise_lists_from_db(read_query_all(conn, 'SELECT symbol FROM sp500_list'))
    symbols_in_db = normalise_lists_from_db(read_query_all(conn, query_get_list_of_tables))

    dl_and_update_d1_data(symbols_sp500, symbols_in_db)

    symbols_in_db_after_load = normalise_lists_from_db(read_query_all(conn, query_get_list_of_tables))

    list_of_empty_tables = [check_for_empty_table(items) for items in symbols_in_db_after_load if
                            check_for_empty_table(items)]

    list_1d_tables_not_in_db = []

    if len(list_of_empty_tables) != 0:
        print(f'Пустые таблицы: {list_of_empty_tables}')
    else:
        print(f'Пустые таблицы в БД отсутствуют')

    # if len(list_tables_not_in_db) != 0:
    #    print(f'Все еще остались не созданы в БД {list_tables_not_in_db}')
    conn.close()
