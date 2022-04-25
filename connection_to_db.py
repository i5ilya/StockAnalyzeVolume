import psycopg2

# Connection parameters

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(dbname='SP500', user='postgres',
                         password='syncmaster', host='localhost')
        print("Connection to PostgreSQL DB successful")
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        # sys.exit(1)



if __name__ == '__main__':
    conn = connect()  # connect to the database
    cursor = conn.cursor()
    conn.close()  # close the connection
