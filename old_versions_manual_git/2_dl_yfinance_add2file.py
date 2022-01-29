import time
import yfinance as yf
import csv

companies = csv.reader(open("../S&P500-Symbols.csv"), delimiter=',')

for company in companies:
    print(company)
    #    print(company[1])
    history_filename = 'history/{}.csv'.format(company[1])
    f = open(history_filename, 'a')
    ticker = yf.Ticker(company[1])
    df = ticker.history(period='5d')
    f.write(df.to_csv())
    f.close()
    time.sleep(2)