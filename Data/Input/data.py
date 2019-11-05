
import pyodbc  # using the pyodbc library 

db_file = r'C:\Users\Hachem\Desktop\Script\data\Input\TESTdb.accdb' #define the location of your Access file

odbc_conn_str = 'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=%s' %(db_file) # define the odbc connection parameter

conn = pyodbc.connect(odbc_conn_str) # establish a database connection

cursor = conn.cursor() # create a cursor
