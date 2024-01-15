import dask.threaded
import MySQLdb
import pandas as pd
from time import time
import configparser

import warnings
warnings.simplefilter(action='ignore', category=UserWarning)

CONFIG_LOC = r'D:\PythonByHarry\py_tools\config.ini'

config = configparser.ConfigParser()
config.read(CONFIG_LOC)
section = 'mt4'
host, user, port, passwd = tuple([config.items(section)[i][1] for i in range(len(config.items(section)))])

def get_data_example(server: str) -> pd.DataFrame:
    '''get number of rows in the given server database
    '''
    query = '''
            SELECT '{server}' AS server, COUNT(*) AS count FROM {server}.mt4_users
            '''.format(server=server)
    sql_connect = MySQLdb.connect(host=host, port=int(port), user=user, passwd=passwd)
    df = pd.read_sql(query, sql_connect)

    return df

if __name__=='__main__':
    servers = ['enfaureport', 'enfau2report', 'enfau3report', 'enfau4report', 'enfau5report', 'enfau6report',
               'enfukreport', 'enfuk2report', 'enfuk3report', 'enfuk4report', 'enfuk5report', 'enfuk6report']
    
    # suppose we want to get data for each server but it's too large to run in one SQL query
    # old way: split large query into small batches and run them in a for loop
    start_time = time()
    df1 = pd.DataFrame()
    for server in servers:
        tmp_df = get_data_example(server=server)
        df1 = pd.concat([df1, tmp_df])
    end_time = time()
    print(f'for loop duration: {(end_time-start_time):.4f}s')

    # new way: use parallel computing functionality of dask package to run each small batch at once
    # the number of tasks being run by dask depends on how many logical processors your CPU has. 8 in this example
    start_time = time()
    values = [dask.delayed(get_data_example)(server=server) for server in servers] 
    df2 = pd.concat(dask.compute(*values, scheduler='threads'))
    end_time = time()
    print(f'dask duration: {(end_time-start_time):.4f}s')
