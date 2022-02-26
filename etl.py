import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """ Method for loading json data from amazon s3 (LOG_DATA and SONG_DATA) and inserting into tables (staging_events and staging_songs)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Method for inserting tables (staging_events and staging_songs) into star schemed analytic tables (songplays as fact table and artists,songs, time, users as dimension tables)
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connecting to database and inserting json data into tables, then inserting tables into star schemed analytic tables 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()