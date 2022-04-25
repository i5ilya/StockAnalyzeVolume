query_get_list_of_tables = '''
    SELECT table_name FROM information_schema.tables
WHERE table_schema NOT IN ('information_schema','pg_catalog');
'''

def query_create_table(table_name):
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
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
        DELETE FROM {ticket} WHERE ctid NOT IN (SELECT max(ctid) FROM {ticket} GROUP BY {row});
        """