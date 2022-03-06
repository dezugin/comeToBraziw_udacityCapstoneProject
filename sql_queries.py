import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_i94_table_drop = "DROP TABLE IF EXISTS staging_i94"
staging_airport_table_drop = "DROP TABLE IF EXISTS staging_airport"
braziliansinairports_table_drop = "DROP TABLE IF EXISTS braziliansinairports"


# CREATE TABLES

staging_i94_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_i94 (
                index_id    INTEGER                 NOT NULL SORTKEY DISTKEY IDENTITY(1, 1),
                cicid       INTEGER                 NULL,
                i94yr       INTEGER                 NULL,
                i94mon      INTEGER                 NULL,
                i94cit      INTEGER                 NULL,
                i94res      INTEGER                 NULL,
                i94port     VARCHAR                 NOT NULL,
                arrdate     INTEGER                 NULL,
                i94mode     INTEGER                 NULL,
                i94addr     VARCHAR                 NULL,
                depdate     INTEGER                 NULL,
                i94bir      INTEGER                 NULL,
                i94visa     INTEGER                 NULL,
                count       INTEGER                 NULL,
                dtadfile    BIGINT                  NULL, 
                visapost    VARCHAR                 NULL,
                occup       VARCHAR                 NULL,
                entdepa     VARCHAR                 NULL,
                entdepd     VARCHAR                 NULL,
                entdepu     VARCHAR                 NULL,
                matflag     VARCHAR                 NULL,
                biryear     INTEGER                 NULL,
                dtaddto     BIGINT                  NULL,
                gender      VARCHAR                 NULL,
                isnum       VARCHAR                 NULL,
                airline     VARCHAR                 NULL,
                admnum      BIGINT                  NULL,
                fltno       INTEGER                 NULL,
                visatype    VARCHAR                 NULL
    );
""")

staging_airport_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_airport (
                ident               VARCHAR         NOT NULL SORTKEY DISTKEY,
                type                VARCHAR         NULL,
                name                VARCHAR         NULL,
                elevation_ft        VARCHAR         NULL,
                continent           VARCHAR         NULL,
                iso_country         VARCHAR         NULL,
                iso_region          VARCHAR         NULL,
                municipality        VARCHAR         NOT NULL,
                gps_code            VARCHAR         NULL,
                iata_code           VARCHAR         NOT NULL,
                local_code          VARCHAR         NULL,
                coordinates         VARCHAR         NOT NULL
    );
""")

                

braziliansinairports_table_create = ("""
    CREATE TABLE IF NOT EXISTS braziliansinairports (
                municipality    VARCHAR             NOT NULL,
                lat             VARCHAR             NOT NULL DISTKEY SORTKEY,
                lon             VARCHAR             NOT NULL,
                count           INTEGER             NOT NULL
    );
""")

# STAGING TABLES

staging_airport_copy = """
COPY staging_airport
FROM 's3://astrogildopereirajunior/airport-codes_csv.csv'
credentials 'aws_iam_role={}'
csv;""".format(config.get('IAM_ROLE', 'ARN'))

staging_i94_copy = """
    COPY staging_i94
    FROM 's3://astrogildopereirajunior/staging_i94.csv'
    credentials 'aws_iam_role={}'
    csv;
""".format(config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
braziliansinairports_table_insert("""
    INSERT INTO braziliansinairports(
                i94ports      as airport
                municipality    VARCHAR,
                lat             VARCHAR,
                lon             VARCHAR,
                count           INTEGER,
    )
    SELECT i.i94ports,a.municipality, a.coordinates, a.iata_code,count(*) 
    REPLACE(REPLACE (i.i94ports, 'NYC','JFK'),'LOS','LAX')
    FROM staging_i94 AS i
    WHERE i94res = 689
    INNER JOIN staging_airports AS a
    ON i.i94ports = a.iata_code
    GROUP BY a.municipality, a.coordinates, a.iata_code
""")

# QUERY LISTS

create_table_queries = [staging_i94_table_create, staging_airport_table_create, braziliansinairports_table_create]
drop_table_queries = [staging_i94_table_drop, staging_airport_table_drop, braziliansinairports_table_drop]
copy_table_queries = [staging_airport_copy,staging_i94_copy]
insert_table_queries = [braziliansinairports_table_insert]