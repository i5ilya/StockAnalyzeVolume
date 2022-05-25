import pandas as pd


def volume_price_level_calc(pd_df):
    """
    Функция находит уровень цен (минимальная цена и максимальная)
    в котором находится наибольший проторгованный объем.
    :param pd_df: На входе pandas dataframe.
    :return: На выходе: два числа float - минимум и максимум цен, которые образуют в дальнейшем уровень цен.
    """
    #  Делаем копию dataframe
    pd_df = pd_df.copy()
    #  Создаем новый столбец 'Price Groups', в котором все цены будут поделены на группы (количество групп: bins=30)
    pd_df['Price Groups'] = pd.cut(pd_df['Close'], bins=30)
    #  Группируем новую таблицу по столбцу 'Price Groups', при этом суммируем и объединяем все объемы по 'Price Groups'
    pd_df = pd_df.groupby(['Price Groups'])['Volume'].sum().reset_index()
    # Сортируем новую таблицу по столбцу 'Volume', наибольшие объемы вверху таблицы
    sorted_pd_df = pd_df.sort_values(by='Volume', ascending=False)
    #  Делаем табличку short с тремя верхними диапазонами, которые имеют самые большие объемы
    short = sorted_pd_df.head(3)
    #  Выбираем нижнюю и верхнюю границы этих трех диапазонов цен
    max_price = short['Price Groups'].max()
    min_price = short['Price Groups'].min()
    return min_price.left, max_price.right
