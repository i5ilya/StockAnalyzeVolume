# https://medium.com/wealthy-bytes/5-lines-of-python-to-automate-getting-the-s-p-500-95a632e5e567

import pandas as pd

table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

df = table[0]

dfy = df['Symbol'].str.replace('.', '-', regex=True)  # У yahoo finance в некоторых тикерах символы с точкой идут через тире.

# df.to_csv('S&P500-Info.csv')
dfy.to_csv("S&P500-Symbols.csv", columns=['Symbol'], header=False)
