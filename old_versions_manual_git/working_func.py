import csv
import numpy


def dict_from_csv():  # функция из файла делает словарь, ключ - это объем, данные - это дата.
    with open("history/MRNA1.csv") as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        dict_from_csv = {}
        for row in reader:
            dict_from_csv[row['Volume']] = row['Date']
        return dict_from_csv
# print(dict_from_csv())


def list_from_csv_vol():  # Функция из столбца файла делает список, преобразует в цифры.
    with open("history/MRNA1.csv") as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        volume_as_list = []
        for row in reader:
            volume_as_list.append(row['Volume'])  # Столбец с объемом превращаем в список
    if 'Volume' in list_from_csv_vol:
        list_from_csv_vol.remove('Volume')
    for i in range(0, len(volume_as_list)):
        volume_as_list[i] = int(volume_as_list[i])  # преобразуем элементы списка из строк в int
    return volume_as_list


a = max(list_from_csv_vol()[-30:-1])
b = dict_from_csv()[str(a)]

average_of_volume = numpy.round(numpy.mean(list_from_csv_vol()))

for x in list_from_csv_vol():
    if x >= average_of_volume * 3:
        print(dict_from_csv()[str(x)])

