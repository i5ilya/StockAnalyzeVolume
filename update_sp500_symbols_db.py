from connection import connect
from func_exec_and_read_querys_db import read_query_all, execute_query, copy_from_stringio
import pandas as pd

conn = connect()


def normalise_lists_from_db(raw_list):
    return [item[0] for item in raw_list]


table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
df = table[0]
dfy = df['Symbol'].str.replace('.', '-',
                               regex=True)  # У yahoo finance в некоторых тикерах символы с точкой идут через тире.

symbols_sp500 = normalise_lists_from_db(read_query_all(conn, 'SELECT symbol FROM sp500_list'))
symbols_in_wiki = dfy.tolist()

for symbol in symbols_in_wiki:
    if symbol not in symbols_sp500:
        print(f'{symbol} добавлен в список на википедии')

for symbol in symbols_sp500:
    if symbol not in symbols_in_wiki:
        print(f'{symbol} удален из списка на википедии')

try:
    value = int(input('Для обновления списка акций из вики в базу нажми "1": '))
    if value == 1:
        execute_query(conn, 'delete from sp500_list')
        copy_from_stringio(conn, dfy, 'sp500_list')
        for symbol in symbols_sp500:
            if symbol not in symbols_in_wiki:
                print(f'{symbol} удален из списка на википедии, удаляем из базы')
                execute_query(conn, f'DROP TABLE IF EXISTS "{symbol.lower()}_1d", "{symbol.lower()}_5m"')
    else:
        print('Ничего не делаем')
except ValueError:
    print('Введены не цифры: ')

# execute_query(conn, 'delete from sp500_list')
# copy_from_stringio(conn, dfy, 'sp500_list')


conn.close()
# if __name__ == '__main__':
#     cursor = conn.cursor()
