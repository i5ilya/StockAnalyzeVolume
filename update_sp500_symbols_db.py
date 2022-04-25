from connection_to_db import connect
from func_exec_and_read_querys_db import read_query_all, execute_query, copy_from_stringio
import pandas as pd


conn = connect()


table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
df = table[0]
dfy = df['Symbol'].str.replace('.', '-',
                               regex=True)  # У yahoo finance в некоторых тикерах символы с точкой идут через тире.


execute_query(conn, 'delete from sp500_list')
copy_from_stringio(conn, dfy, 'sp500_list')


conn.close()
# if __name__ == '__main__':
#     cursor = conn.cursor()
