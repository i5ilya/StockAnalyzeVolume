import csv
import numpy



def dict_from_csv():  # функция из файла делает словарь, ключ - это объем, данные - это дата.
    with open('../history/UDR.csv') as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        dict_from_csv = {}
        for row in reader:
            dict_from_csv[row['Volume']] = row['Date']
        return dict_from_csv


def list_from_csv_vol():  # Функция из столбца файла делает список, преобразует в цифры.
    with open('../history/UDR.csv') as File:
        reader = csv.DictReader(File)  # открытие файла как словаря
        list_from_csv_vol = []
        for row in reader:
            list_from_csv_vol.append(row['Volume'])  # Столбец с объемом превращаем в список
    # duty_list = 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock', 'Splits'
    # while x in duty_list:
    if 'Volume' in list_from_csv_vol:
        list_from_csv_vol.remove('Volume')
    if '.0' in list_from_csv_vol:
        list_from_csv_vol.remove('.0')

    try:
        for i in range(0, len(list_from_csv_vol)):
            list_from_csv_vol[i] = float(list_from_csv_vol[i]) # преобразуем элементы списка из строк в int
            list_from_csv_vol[i] = int(list_from_csv_vol[i])
    except (ZeroDivisionError, TypeError, IndexError, ValueError):
        print('File')
    return list_from_csv_vol
# print (list_from_csv_vol())
average_of_volume = numpy.round(numpy.mean(list_from_csv_vol()))
for x in list_from_csv_vol()[-20:-1]:
    if x >= average_of_volume * 3:
        print(dict_from_csv()[str(x)])


