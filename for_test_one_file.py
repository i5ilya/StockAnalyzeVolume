import csv
import numpy


def dict_from_csv():  # функция из файла делает словарь, ключ - это объем, данные - это дата.
    with open("history/MAA.csv") as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        dict_from_csv = {}
        for row in reader:
            dict_from_csv[row['Volume']] = row['Date']
        for key in list(dict_from_csv.keys()):  # Yahoofinance иногда выдает дату и дальше пусто.
            if key == '':
                del dict_from_csv['']
        dict_from_csv = {int(float(k)): str(v) for k, v in dict_from_csv.items()}  # Преобразуем объем в число.
    return dict_from_csv


def list_from_csv_vol():  # Функция из столбца файла делает список, преобразует в цифры.
    with open("history/MAA.csv") as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        list_from_csv_vol = []
        for row in reader:
            list_from_csv_vol.append(row['Volume'])  # Столбец с объемом превращаем в список


    if '' in list_from_csv_vol:
        list_from_csv_vol.remove('')


    for i in range(0, len(list_from_csv_vol)):
        list_from_csv_vol[i] = int(float(list_from_csv_vol[i]))  # преобразуем элементы списка из строк в int
    return list_from_csv_vol
# print(list_from_csv_vol())
average_of_volume = numpy.round(numpy.mean(list_from_csv_vol()))
for x in list_from_csv_vol()[-20:-1]:
    if x >= average_of_volume * 3:
        #print(dict_from_csv()[str(x)])
        print(dict_from_csv()[x])