# StockAnalyzeVolume

This simple program searches for stocks included in the [S&P 500 index](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) for a surge of the largest horizontal volumes on the daily price chart, followed by the calculation of the price level (minimum and maximum), where this volume is located.  

Brief logic:
* First we find the horizontal volume spike (1).
* Then we calculate the price level (2).
* We draw a graph where the search results are marked with lines (using the IBM graph as an example):  

![IBM](/examples/IBM.png)  

**By the way, this chart shows further price reaction to this level.**  

The most interesting (in my opinion) function that calculates the price level. Written using the Pandas framework:

```python
def volume_price_level_calc(pd_df):
    """
    The function finds the price level (minimum price and maximum)
    which contains the largest traded volume.
    :param pd_df: Input - pandas dataframe.
    :return: two float numbers - minimum and maximum prices, which form the price level in the future
    """
    #  Making a copy of dataframe
    pd_df = pd_df.copy()
    #  Create a new column 'Price Groups', in which all prices will be divided into groups (number of groups: bins=30)
    pd_df['Price Groups'] = pd.cut(pd_df['close'], bins=30)
    #  Grouping a new table by the 'Price Groups' column, while summing and merging all volumes by 'Price Groups'
    pd_df = pd_df.groupby(['Price Groups'])['volume'].sum().reset_index()
    # Sort the new table by the 'Volume' column, the largest volumes are at the top of the table
    sorted_pd_df = pd_df.sort_values(by='volume', ascending=False)
    #  We make a short table with three upper ranges that have the largest volumes
    short = sorted_pd_df.head(3)
    #  We select the lower and upper limits of these three price ranges
    max_price = short['Price Groups'].max()
    min_price = short['Price Groups'].min()
    return [min_price.left, max_price.right]
```

Well, I'll describe the whole mechanism of the program:  

1. First of all, we read a table of stocks from the S&P 500 index from [Wikipedia](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies) (after all, this list is not static and may change). At the same time, we check and display on the screen which shares have been removed from the list, and which ones have been added.
2. Write the list of stocks to the database.
3. The next step is to download the data for this list of stocks. The yfinance framework was chosen for data loading. The framework already gives us a dataframe, which we save in the database.
4. This is what the data in the database looks like: ![DB_1D](/examples/db_d1.PNG)
5. To save data to the database, the fastest (due to the large volume of tables) method of directly writing to the database from memory using StringIO () was chosen, after reading this article with a benchmark: [Pandas to PostgreSQL using Psycopg2: Bulk Insert Performance Benchmark | Naysan Saran](https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/)
6. Implemented functions for updating the database. To add data to the database, we only add data since the last load (we look at the last entry in the database by the date column). After that, checks for the presence of duplicate records (by date) were also added.
7. Next, we run a module that finds only a volume spike (which is 3 or more times larger than the ones next to it). At the same time, we are not interested in all surges, but in those that have occurred over the past month. This module returns us a dictionary, where the key is the ticket, and the value is the date or a list of dates, with bursts of volumes of interest to us:
```python
'AMCR': ['2022-07-29'], 'BALL': ['2022-08-04', '2022-08-05']
```
8. After that, we need to find the desired price level in each of the days. First of all, we download the data for this day, but it is already a 5-minute chart (one candle per 5 minutes). The data for the 5 minute chart looks like this: ![DB_1D](/examples/db_m5.PNG)
9. After downloading all the data, we go through the dictionary from paragraph 7, calculate the price level (function above), add more values to the keys in the form of a list (level) of prices. And our dictionary looks like this:
```python
 'AMCR': (['2022-07-29'], [12.938, 13.01]), 'BALL': (['2022-08-04', '2022-08-05'], [59.039, 59.592, 56.304, 56.413]).
```
10. Finally, we substitute this dictionary in a loop into a simple function made using the [mplfinance framework](https://github.com/matplotlib/mplfinance):
```python
def mplfinance_plotter(ticker, dataframe_d1, list_h_lines, list_v_lines):
    print(f'Creating fig {ticker}...')
    fig, axes = mpf.plot(dataframe_d1, type='candle', volume=True,
                         hlines=dict(hlines=list_h_lines, linewidths=0.5),
                         vlines=dict(vlines=list_v_lines, linewidths=0.5), returnfig=True)
    axes[0].set_title(ticker)
    print(f'Saving fig {ticker} on disk...')
    fig.savefig(f"img/{ticker}.png")
    plt.close(fig)
```
>So, what's next? )
>Next is the automation of the search for patterns of price movement from this level. Perhaps cryptocurrencies... Is it worth it?) I'm not sure...


## A few more examples of how the price reacts to the level:
![AMT](/examples/AMT.png) 
![SHV](/examples/SHW.png) 
![UNH](/examples/UNH.png)
![WY](/examples/WY.png)

