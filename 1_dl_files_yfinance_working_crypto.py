import yfinance as yf
import csv
import time

companies = csv.reader(open("Crypto_list_yahoo.csv"))

for company in companies:
    print(company[0])
    history_filename = 'history_crypto/{}.csv'.format(company[0])
    f = open(history_filename, 'w')
    ticker = yf.download(company[0], period='6mo')
    # df = ticker.history(period='6mo')
    # df = ticker
    f.write(ticker.to_csv())
    f.close()
    #  time.sleep(3)
