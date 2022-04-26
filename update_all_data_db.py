from connection_to_db import connect
from queries import query_get_list_of_tables
from func_exec_and_read_querys_db import read_query_all, execute_query, copy_from_stringio, read_query_one, \
    dl_data_yf_period, query_delete_duplicates
from queries import query_get_list_of_tables, query_create_table
import yfinance as yf
import pandas as pd
import datetime
import time

conn = connect()
symbols_in_db_raw = read_query_all(conn, query_get_list_of_tables)
symbols_in_db = []
for symbol in symbols_in_db_raw:
    symbols_in_db.append(symbol[0].lower())

symbols_raw = read_query_all(conn, 'SELECT symbol FROM sp500_list')
symbols = []
for symbol in symbols_raw:
    symbols.append(symbol[0].lower())

problem_with_data_load = []

for symbol in symbols:
    if symbol not in symbols_in_db:
        print(f'Символа {symbol}, нет в базе данных, скачиваем...')
        df = yf.download(symbol, period='6mo')
        print(f'Создаем таблицу {symbol}...')
        execute_query(conn, query_create_table(symbol))
        print(f'Заполняем таблицу данными {symbol}...')
        copy_from_stringio(conn, df, symbol)
    else:
        print(f'Символ {symbol} присутствует в базе, проверяем последнюю запись в базе...')
        last_date = read_query_one(conn, f'SELECT max(date) FROM "{symbol}"')
        if last_date[0] is not None:
            print(f'База данных {symbol} содержит последнюю запись от {last_date[0]}')
            #days_plus_one_from_db = last_date[0] + datetime.timedelta(days=1) # если грузим повторно в рабочий день, то день с заврашний)
            print(f'Скачиваем данные с {last_date[0]} по {datetime.datetime.today()}... ')
            df_d = dl_data_yf_period(symbol, last_date[0], datetime.datetime.today())
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
            if last_date[0] is not None:
                print(f'Кажется, база заполнилась)')
            else:
                print(f'Какие-то проблемы с загрузкой {symbol}')
                problem_with_data_load.append(symbol)

symbols_in_db_raw_after_all = read_query_all(conn, query_get_list_of_tables)
symbols_in_db_after_all = []
for symbol in symbols_in_db_raw:
    symbols_in_db_after_all.append(symbol[0].lower())

left_after_all =[]
for symbol in symbols:
    if symbol not in symbols_in_db_after_all:
        left_after_all.append(symbol)

if not problem_with_data_load:
    print(f'Какие-то проблемы с загрузкой: {problem_with_data_load}')

if not symbols_in_db_after_all:
    print(f'Все еще остались не загружены и не созданы в БД {left_after_all}')

