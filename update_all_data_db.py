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

symbols_in_db_raw = read_query_all(conn, query_get_list_of_tables)
symbols_sp500raw = read_query_all(conn, 'SELECT symbol FROM sp500_list')


def normalise_lists_from_db(raw_list):
    return [item[0].lower() for item in raw_list]

symbols_sp500 = normalise_lists_from_db(symbols_sp500raw)
symbols_now_in_db = normalise_lists_from_db(symbols_in_db_raw)


problem_with_data_load = []


def dl_data_from_yahoo_to_db(symbols_to_dl, symbols_in_db):

    for symbol in symbols_to_dl:
        if symbol not in symbols_in_db:
            print(f'Символа {symbol}, нет в базе данных, скачиваем...')
            df = yf.download(symbol, period='6mo')
            print(f'Создаем таблицу {symbol}...')
            execute_query(conn, query_create_table(symbol))
            print(f'Заполняем таблицу данными {symbol}...')
            copy_from_stringio(conn, df, symbol)
        else:
            print(f'Символ {symbol} присутствует в базе, проверяем последнюю запись в базе...')
            last_date_raw = read_query_one(conn, f'SELECT max(date) FROM "{symbol}"')
            last_date = last_date_raw[0].date()
            if last_date != datetime.date.today() and last_date != datetime.date.today() - datetime.timedelta(days=1):
                if last_date is not None:
                    print(f'База данных {symbol} содержит последнюю запись от {last_date}')
                    #  days_plus_one_from_db = last_date[0] + datetime.timedelta(days=1) # если грузим повторно в рабочий день, то день с заврашний)
                    print(f'Скачиваем данные с {last_date} по {datetime.date.today()}... ')
                    df_d = dl_data_yf_period(symbol, last_date, datetime.date.today())
                    print(f'Загружаем полученные данные по {symbol} в БД...')
                    copy_from_stringio(conn, df_d, symbol)
                    print(f'Удаляем возможные дубли в базе {symbol}')
                    execute_query(conn, query_delete_duplicates(symbol, 'date'))
                else:
                    print(f'База данных {symbol} существует, но она пустая! Пробуем скачать данные еще раз...')
                    df = yf.download(symbol, period='6mo')
                    print(f'Заполняем таблицу данными {symbol}...')
                    copy_from_stringio(conn, df, symbol)
                    last_date = read_query_one(conn, f'SELECT max(date) FROM {symbol}')
                    if last_date is not None:
                        print(f'Кажется, база заполнилась)')
                    else:
                        print(f'Какие-то проблемы с загрузкой {symbol}')
                        problem_with_data_load.append(symbol)
            else:
                print(f'База данных {symbol} содержит последнюю запись от {last_date}')
                print(f'Данные {symbol} в БД актуальны и не требуют загрузки')



dl_data_from_yahoo_to_db(symbols_sp500, symbols_now_in_db)

symbols_in_db_raw_after_all = read_query_all(conn, query_get_list_of_tables)
symbols_in_db_after_all = normalise_lists_from_db(symbols_in_db_raw_after_all)

left_after_all = []
for item in symbols_sp500:
    if item not in symbols_in_db_after_all:
        left_after_all.append(item)

if len(problem_with_data_load) != 0:
    print(f'Какие-то проблемы с загрузкой: {problem_with_data_load}')

if len(symbols_in_db_after_all) != 0:
    print(f'Все еще остались не загружены и не созданы в БД {left_after_all}')
conn.close()
