import csv
import numpy
import os
# import pandas as pd


path = 'history'
list_files = os.listdir(path)
# print(list_files)

for file in list_files:
    #  print(path+'\\'+file)
    #  print(file, dict_from_csv(file))
    def dict_from_csv(file):  # функция из файла делает словарь, ключ - это объем, данные - это дата.
        with open(path+'\\'+file) as File:
            reader = csv.DictReader(File)  # открытие файла как словаря
            dict_from_csv = {}
            for row in reader:
                dict_from_csv[row['Volume']] = row['Date']
            return dict_from_csv

    def list_from_csv_vol(file):  # Функция из столбца файла делает список, преобразует в цифры.
        with open(path+'\\'+file) as File:
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


    average_of_volume = numpy.round(numpy.mean(list_from_csv_vol(file)))
    #  print(file, average_of_volume)
    for x in list_from_csv_vol(file)[-20:-1]:
        if x >= average_of_volume * 3:
            print(file, dict_from_csv(file)[str(x)])
