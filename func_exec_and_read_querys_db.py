from io import StringIO
import psycopg2.extras
import yfinance as yf


def query_create_table(table_name):
    return f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
    Date timestamp,
    Open double precision,
    High double precision,
    Low double precision,
    Close double precision,
    "Adj Close" double precision,
    Volume bigint
    )
    """


def query_delete_duplicates(ticket, row):
    return f"""
        DELETE FROM "{ticket}" WHERE ctid NOT IN (SELECT max(ctid) FROM "{ticket}" GROUP BY {row});
        """


def execute_query(conn, query):
    # connection.autocommit = True
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)

    try:
        cursor.execute(query)
        conn.commit()
        if len(conn.notices) != 0:
            print(conn.notices)
        if len(cursor.statusmessage) != 0:
            print(cursor.statusmessage)
        print("Query executed successfully")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    cursor.close()


def read_query_all(conn, query):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    # cursor = conn.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    cursor.close()


def read_query_one(conn, query):
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchone()
        return result
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"The error '{error}' occurred")
    cursor.close()


def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False)
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, f"{table}", sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:

        print(f"The error '{error}' occurred")
        conn.rollback()
        cursor.close()
        return 1
    print("Copy from stringio to DB done")
    cursor.close()


# def dl_data_yf_period(ticket, start_time, end_time):
#     data = None
#     try:
#         data = yf.download(ticket, start=start_time, end=end_time)
#         return data
#     except Exception as error:
#         print(f"The error '{error}' occurred")


def dl_data_yf_period(ticket, start_time, end_time, timeframe: str ='1d'):
    """
    :param ticket: from Yahoo Finance
    :param start_time: Date start
    :param end_time: Date end
    :param timeframe: valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    :return: pandas dataframe
    """
    data = None
    try:
        data = yf.download(ticket, start=start_time, end=end_time, interval=timeframe)
        return data.dropna()
    except Exception as error:
        print(f"The error '{error}' occurred")