import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

""" queries that loads data from S3 buckets
to Redshift
"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Loading data by: '+query)
        cur.execute(query)
        conn.commit()

""" INSERT statements from staging tables to 
the dimension and fact tables
"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Transform data by: '+query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
  
    print('Connecting to redshift')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to redshift')
    cur = conn.cursor()
    
    print('Loading staging tables')
    #load_staging_tables(cur, conn)
    
    print('Transform from staging')
    insert_tables(cur, conn)

    conn.close()
    print('ETL Ended')


if __name__ == "__main__":
    main()