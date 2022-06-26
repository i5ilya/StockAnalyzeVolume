from connection import Tables
from func_exec_and_read_querys_db import dl_data_yf_period, query_delete_duplicates
from queries import query_get_list_of_tables, query_create_table
from datetime import datetime
import datetime

db = Tables('SP500')


def normalise_lists_from_db(raw_list):
    return [item[0].lower() for item in raw_list]


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
        db.copy_from_stringio(df, symbol + '_' + tf)
        print(f'Удаляем возможные дубли в базе {symbol}_{tf}')
        db.execute_query(query_delete_duplicates(symbol + '_' + tf, 'date'))
    else:
        print(f'Создаем таблицу {symbol}_{tf}...')
        db.execute_query(query_create_table(symbol + '_' + tf))
        print(f'Заполняем таблицу данными {symbol}_{tf}...')
        db.copy_from_stringio(df, symbol + '_' + tf)
        print(f'Удаляем возможные дубли в базе {symbol}_{tf}')
        db.execute_query(query_delete_duplicates(symbol + '_' + tf, 'date'))


def dl_and_update_d1_data(symbols_to_dl, symbols_in_database):
    tf = '1d'
    for symbol in symbols_to_dl:
        if symbol + '_' + tf not in symbols_in_database:
            print(f'Символа {symbol}_{tf}, нет в базе данных...')
            dl_data_to_db(symbol, symbols_in_database, datetime.date.today() - datetime.timedelta(days=180),
                          datetime.date.today())
        else:
            last_date_raw = db.fetch_one(f'SELECT max(date) FROM "{symbol}_{tf}"')
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
        lastdate_in_table = db.fetch_one(f'SELECT max(date) FROM "{table_name}"')
        if lastdate_in_table is not None:
            return False
        else:
            return True


if __name__ == '__main__':

    symbols_sp500 = normalise_lists_from_db(db.fetch_all('SELECT symbol FROM sp500_list'))
    symbols_in_db = normalise_lists_from_db(db.fetch_all(query_get_list_of_tables))

    dl_and_update_d1_data(symbols_sp500, symbols_in_db)

    symbols_in_db_after_load = normalise_lists_from_db(db.fetch_all(query_get_list_of_tables))

    list_of_empty_tables = [check_for_empty_table(items) for items in symbols_in_db_after_load if
                            check_for_empty_table(items)]

    list_1d_tables_not_in_db = []

    if len(list_of_empty_tables) != 0:
        print(f'Пустые таблицы: {list_of_empty_tables}')
    else:
        print(f'Пустые таблицы в БД отсутствуют')

    # if len(list_tables_not_in_db) != 0:
    #    print(f'Все еще остались не созданы в БД {list_tables_not_in_db}')
