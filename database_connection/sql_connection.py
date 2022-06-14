import configparser
import pyodbc
import pandas as pd


def get_connection(config_path, config_section='DEFAULT'):

    config = configparser.ConfigParser()
    config.read(config_path)

    server = config[config_section]['host']
    username = config[config_section]['user']
    password = config[config_section]['password']
    trusted_connection = config[config_section]['trusted_connection']
    driver = config[config_section]['driver']
    tds_version = config[config_section]['tds_version']
    port = config[config_section]['port']
    Encrypt = True

    cnxn = pyodbc.connect(server=server,
                          user=username,
                          tds_version=tds_version,
                          password=password,
                          port=port,
                          driver=driver,
                          Trusted_connection=trusted_connection,
                          Encrypt=Encrypt)
    return cnxn, cnxn.cursor()


def run_sql(sql_loc, cursor, sql_vars=None):
    with open(sql_loc) as query:
        sql = query.read()

    if sql_vars:
        cursor.execute(sql.format(**sql_vars))
    else:
        cursor.execute(sql)

    return cursor, cursor.fetchall()


def run_sql_text_query(query, cursor):
    try:
        cursor.execute(query)
        return cursor, cursor.fetchall()
    except TypeError as e:
        print("The entered query must be in a string format")
        raise e


def row_to_df(rows, cursor):
    return pd.DataFrame.from_records(rows, columns=[d[0] for d in cursor.description])
