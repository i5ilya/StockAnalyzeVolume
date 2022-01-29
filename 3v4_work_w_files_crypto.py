import csv
# import numpy
import os
# import pandas as pd
# from collections import OrderedDict


def dict_from_csv(file_d):  # функция из файла делает словарь, ключ - это объем, данные - это дата.
    with open(path + '\\' + file_d) as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        dict_from_csv = {}
        for row in reader:
            dict_from_csv[row['Volume']] = row['Date']
    for key in list(dict_from_csv.keys()):  # Yahoofinance иногда выдает дату и дальше пусто.
        if key == '':
            del dict_from_csv['']
    dict_from_csv = {int(float(k)): str(v) for k, v in dict_from_csv.items()}  # обходим словарь и преобразуем объем в число.

    return dict_from_csv


def list_from_csv_vol(file_v):  # Функция из столбца файла делает список, преобразует в цифры.
    with open(path+'\\'+file_v) as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        list_from_csv_vol = []
        for row in reader:
            list_from_csv_vol.append(row['Volume'])  # Столбец с объемом превращаем в список
    if '' in list_from_csv_vol:
        list_from_csv_vol.remove('')

    try:
        for i in range(0, len(list_from_csv_vol)):
            list_from_csv_vol[i] = int(float(list_from_csv_vol[i]))  # преобразуем элементы списка из строк в float
            # list_from_csv_vol[i] = int(list_from_csv_vol[i])  # преобразуем элементы списка из float в int
    except (ZeroDivisionError, TypeError, IndexError, ValueError):
        print('Ошибка преобразования элемента списка из строк в int в файле: ' + str(File))
    return list_from_csv_vol

    #  print(file, list_from_csv_vol(file))


path = 'history_crypto'
list_files = os.listdir(path)
# print(list_files)
findedlist = []
for file in list_files:
    try:
        if len(list_from_csv_vol(file)) >= 150:  # не смотрим тех, чья история меньше 5 месяцев (30*5)
            #  print(file, dict_from_csv(file))
            average_of_volume = sum(list_from_csv_vol(file)) / len(list_from_csv_vol(file))
            #  average_of_volume = numpy.round(numpy.mean(list_from_csv_vol(file)))
            #  print(file, average_of_volume)
            for x in list_from_csv_vol(file)[-30:-1]:
                if x >= average_of_volume * 5:
                    print(file, dict_from_csv(file)[x])
                    findedlist.append(file)
            # print(list_crypto_names)       # print(file, dict_from_csv(file)[str(x)])
    except (ZeroDivisionError, TypeError, IndexError, KeyError):
        print('Ошибка расчета среднего объема в файле: ' + file)

finderlist_wo_dubles = list(set(findedlist))
finderlist_wo_dubles.sort()

for i in finderlist_wo_dubles:
    stripted_finderlist_wo_dubles = i.strip('.csv')
    #print(stripted_finderlist_wo_dubles)
