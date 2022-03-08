import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
import boto3
import json

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                   )
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
def manipulate_files(KEY,SECRET):
    """Method for downloading and reading sas format file, as well as converting it to csv while filling it with na, and uploading it to amazon s3
    """
     s3d = boto3.client('s3',
                           region_name="us-east-1",
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                       )
    session = boto3.Session(
                       region_name="us-east-1",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
    )
    s3u = session.resource('s3')
    print("Downloading SAS file")
    s3d.download_file('astrogildopereirajunior', 'i94_apr16_sub.sas7bdat', 'i94.sas7bdat')
    print("SAS file downloaded, reading sas file")
    df = pd.read_sas('i94.sas7bdat', 'sas7bdat', encoding="ISO-8859-1")
    print("sas file read, filling na")
    df = df.fillna(0)
    print("na filled, converting to csv")
    df.to_csv("staging_i94.csv",index=False)
    print("csv recorded, uploading to s3")
    result = upload_file('staging_i94.csv','astrogildopereirajunior')
    print("Upload = ",str(result))

def load_staging_tables(cur, conn):
    """ Method for loading csv data from amazon s3 (staging_i94 and airport_codes_csv) and inserting into tables (staging_i94 and staging_airport)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def process_tables(cur, conn):
    """Method for processing staging tables into one final fact table counting the occurrences of brazilians in airports and their latitude, longitude and municipality, and inserting it into a sql database on redshift
    """
    sql = "select * from staging_airport;"
    airport_codes = pd.read_sql_query(sql, conn)
    sql = "select * from staging_i94;"
    df_i94 = pd.read_sql_query(sql, conn)
    brazil = df_i94[df_i94['i94res']==689]
    brazil['i94port'] = brazil['i94port'].str.replace('NYC','JFK')
    brazil['i94port'] = brazil['i94port'].str.replace('LOS','LAX')
    braziliansInAirports = pd.merge(brazil,airport_codes,left_on=['i94port'], right_on=['iata_code'],how='inner')
    braziliansInAirports = braziliansInAirports.groupby(['municipality','coordinates','iata_code']).size().to_frame(name = 'count').reset_index().sort_values(by=['count'],ascending=False)
    df = braziliansInAirports
    new = df["coordinates"].str.split(", ", n = 1, expand = True)
    df['lat'] = new[1]
    df['lon'] = new[0]
    df = df.groupby(['municipality', 'lat','lon']).size().to_frame(name = 'count').reset_index()
    df.to_csv("braziliansinairports.csv",index=False)
    upload_file('braziliansinairports.csv','astrogildopereirajunior')
    cur = conn.cursor()
    query = """
        COPY braziliansinairports
        FROM 's3://astrogildopereirajunior/braziliansinairports.csv'
        credentials 'aws_iam_role={}'
        csv;
    """.format(config.get('IAM_ROLE', 'ARN'))
    cur.execute(query)
    conn.commit()
def main():
    """Connecting to database and inserting json data into tables, then inserting tables into star schemed analytic tables 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')
   

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    manipulate_files(KEY,SECRET)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()