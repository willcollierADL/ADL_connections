import configparser
import pyodbc
import pandas as pd


def get_connection(config_path, config_section='DEFAULT', use_database=False):
    """
    Connect to the database
    :param config_path: path for the configuration file which contains the connection information
    :param config_section: which section of the config file to use
    :param use_database: connect using the database name
    :return:
    """
    config = configparser.ConfigParser()
    config.read(config_path)

    server = config[config_section]['host']
    username = config[config_section]['user']
    password = config[config_section]['password']
    trusted_connection = config[config_section]['trusted_connection']
    driver = config[config_section]['driver']
    tds_version = config[config_section]['tds_version']
    port = config[config_section]['port']
    encrypt = True

    if use_database:
        database = config[config_section]['database']
        cnxn = pyodbc.connect(server=server,
                              user=username,
                              tds_version=tds_version,
                              password=password,
                              port=port,
                              driver=driver,
                              Trusted_connection=trusted_connection,
                              Encrypt=encrypt,
                              database=database)

    else:
        cnxn = pyodbc.connect(server=server,
                              user=username,
                              tds_version=tds_version,
                              password=password,
                              port=port,
                              driver=driver,
                              Trusted_connection=trusted_connection,
                              Encrypt=encrypt)
    return cnxn, cnxn.cursor()


def run_sql(sql_loc, cursor, sql_vars=None):
    """
    load query from file, add any variables and run it
    :param sql_loc: location of the sql file
    :param cursor: the cursor object for the database connection from get connection
    :param sql_vars: variables to add into the sql file text {}
    :return:
    """
    with open(sql_loc) as query:
        sql = query.read()

    if sql_vars:
        cursor.execute(sql.format(**sql_vars))
    else:
        cursor.execute(sql)

    return cursor, cursor.fetchall()


def run_sql_text_query(query, cursor):
    """
    Pass a query directly into the cursor in text format
    :param query: text query for execution
    :param cursor: cursor from the get connection
    :return:
    """
    try:
        cursor.execute(query)
        return cursor, cursor.fetchall()
    except TypeError as e:
        print("The entered query must be in a string format")
        raise e


def row_to_df(rows, cursor):
    """
    Convert the rows returned from a query into a pandas dataframe
    :param rows: returns data from cursor.fetchall in run sql functions
    :param cursor: cursor object
    :return:
    """
    return pd.DataFrame.from_records(rows, columns=[d[0] for d in cursor.description])


def drop_table(cursor, database_name, table_name):
    """
    Drop a table in the database which has been connected to
    :param cursor:
    :param table_name:
    :return:
    """
    sql = f'DROP TABLE IF EXISTS [{database_name}].[dbo].[{table_name}]'
    cursor.execute(sql)

