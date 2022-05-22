import csv
from connection_to_db import connect
import numpy
import os
# import pandas as pd
from datetime import datetime
from queries import query_get_list_of_tables, query_create_table
import datetime
from func_exec_and_read_querys_db import read_query_all, execute_query, copy_from_stringio, read_query_one, \
    dl_data_yf_period, query_delete_duplicates
def dict_from_csv(file_d):  # функция из файла делает словарь, ключ - это объем, данные - это дата.
    with open(path + '\\' + file_d) as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        dict_from_csv = {}
        for row in reader:
            dict_from_csv[row['Volume']] = row['Date']
        return dict_from_csv


def list_from_csv_vol(file_v):  # Функция из столбца файла делает список, преобразует в цифры.
    with open(path+'\\'+file_v) as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        list_from_csv_vol = []
        for row in reader:
            list_from_csv_vol.append(row['Volume'])  # Столбец с объемом превращаем в список
    if 'Volume' in list_from_csv_vol:
        list_from_csv_vol.remove('Volume')
    if '.0' in list_from_csv_vol:
        list_from_csv_vol.remove('.0')
    for i in range(0, len(list_from_csv_vol)):
        list_from_csv_vol[i] = int(list_from_csv_vol[i])  # преобразуем элементы списка из строк в int
    return list_from_csv_vol
    #  print(file, list_from_csv_vol(file))


# path = 'history'
# list_files = os.listdir(path)
# # print(list_files)
#
# try:
#     for file in list_files:
#         #  print(path+'\\'+file)
#         #  print(file, dict_from_csv(file))
#         average_of_volume = sum(list_from_csv_vol(file)) / len(list_from_csv_vol(file))
#         #average_of_volume = numpy.round(numpy.mean(list_from_csv_vol(file)))
#         #  print(file, average_of_volume)
#         for x in list_from_csv_vol(file)[-50:-1]:
#             if x >= average_of_volume * 3:
#                 print(file, dict_from_csv(file)[str(x)])
# except (ZeroDivisionError, TypeError):
#     print('Поймано плохое исключение', + str(file))

conn = connect()
symbols_sp500raw = read_query_all(conn, 'SELECT symbol FROM sp500_list')

def normalise_lists_from_db(raw_list):
    #clean_list = []
    #clean_list =  [item[0].lower() for item in raw_list]

    return [item[0].lower() for item in raw_list]

symbols_sp500 = normalise_lists_from_db(symbols_sp500raw)

print(symbols_sp500)

conn.close()