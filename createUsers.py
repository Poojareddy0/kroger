import mysql.connector
import pandas as pd
from mysql.connector import errorcode

config = {
    'host': 'serverforfinal.mysql.database.azure.com',
    'user': 'pooja',
    'password':'Putta_123',
    'database':'serverforfinal'
    }
try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

cursor = conn.cursor()

cursor.execute("DROP TABLE IF EXISTS users;")
print("Finished dropping table (if existed).")

cursor.execute("CREATE TABLE users (username varchar(255),password varchar(255),firstName varchar(255), lastName varchar(255), email varchar(255), PRIMARY KEY (username))")
print("Table created successfully")

conn.commit()
cursor.close()
conn.close()