import yfinance as yf
import csv
import time

companies = csv.reader(open("S&P500-Symbols.csv"), delimiter=',')

for company in companies:
    print(company)
    #    print(company[1])
    history_filename = 'history/{}.csv'.format(company[1])
    f = open(history_filename, 'w')
    #ticker = yf.Ticker(company[1])
    ticker = yf.download(company[1], period='6mo')
    # df = ticker.history(period='6mo')
    # df = ticker
    f.write(ticker.to_csv())
    f.close()
    #time.sleep(2)
