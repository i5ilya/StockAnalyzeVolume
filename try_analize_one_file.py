import csv
import numpy
import pandas as pd
#stock_data = csv.reader(open("history/MRNA.csv"), delimiter=',')
import os

with open("history/MRNA.csv") as File:
    reader = csv.DictReader(File)  # открытие файла как словаря
    volume_as_list = []
    #reader = csv.DictReader(File, delimiter=',', quotechar=',')   # не нужно
    #new_reader = filter(bool, reader)  # удаляем пустые строки  # не нужно
    dict_from_csv = {}
    for row in reader:
       #print(row['Date'], row['Volume'])
        volume_as_list.append(row['Volume'])  # Столбец с объемом превращаем в список
        ict_from_csv = {}  # инициализируем пустой словарь
        dict_from_csv[row['Volume']] = row['Date']  # добавление в словарь по ключу Объем

for i in range(0, len(volume_as_list)):
    volume_as_list[i] = int(volume_as_list[i])  # преобразуем элементы списка из строк в integer

print(str(max(volume_as_list[-30:-1])))
#print(str(max(volume_as_list)))
#a = numpy.mean(volume_as_list)
# print(a)
print(dict_from_csv  [ (str(max(volume_as_list[-30:-1])))])