import psycopg2
from io import StringIO


# Connection parameters

# def connect():
#     """ Connect to the PostgreSQL database server """
#     conn = None
#     try:
#         # connect to the PostgreSQL server
#         print('Connecting to the PostgreSQL database...')
#         conn = psycopg2.connect(dbname='SP500', user='postgres',
#                                 password='syncmaster', host='localhost')
#         print("Connection to PostgreSQL DB successful")
#         return conn
#     except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
#         # sys.exit(1)


class Database:

    def __init__(self, dbname):
        self.dbname = dbname
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(dbname=dbname, user='postgres',  # dbname='SP500'
                                         password='syncmaster', host='localhost')
            print("Connection to PostgreSQL DB successful")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        self.cursor = self.conn.cursor(
            cursor_factory=psycopg2.extras.NamedTupleCursor)

    def connector(self):
        return self.conn

    def cursor(self):
        return self.cursor

    def execute(self, query):
        self.cursor.execute(query)

    def __del__(self):
        self.conn.close()
        print('Connection closed')


class Tables(Database):

    def __int__(self, dbname):
        super().cursor()

    def fetch_one(self, query):
        result = None
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"The error '{error}' occurred")

    def fetch_all(self, query):
        result = None
        try:
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            return result
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"The error '{error}' occurred")

    def execute_query(self, query):
        try:
            self.cursor.execute(query)
            self.conn.commit()
            if len(self.conn.notices) != 0:
                print(self.conn.notices)
            if len(self.cursor.statusmessage) != 0:
                print(self.cursor.statusmessage)
            print("Query executed successfully")
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"The error '{error}' occurred")

    def copy_from_stringio(self, df, table):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        # save dataframe to an in memory buffer
        buffer = StringIO()
        df.to_csv(buffer, header=False)
        buffer.seek(0)
        try:
            self.cursor.copy_from(buffer, f"{table}", sep=",")
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"The error '{error}' occurred")
            self.conn.rollback()
            return 1
        print("Copy from stringio to DB done")

    def __del__(self):
        self.cursor.close()
        print('Cursor closed')
        self.conn.close()
        print('Connection closed')



# if __name__ == '__main__':
#     conn = connect()  # connect to the database
#     cursor = conn.cursor()
#     conn.close()  # close the connection
