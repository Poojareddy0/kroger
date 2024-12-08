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

cursor.execute("DROP TABLE IF EXISTS HOUSEHOLDS;")
print("Finished dropping table (if existed).")

cursor.execute("CREATE TABLE HOUSEHOLDS (HSHD_NUM INT, L VARCHAR(50), AGE_RANGE VARCHAR(50), MARITAL varchar(255),INCOME_RANGE varchar(255),HOMEOWNER varchar(255),HSHD_COMPOSITION varchar(255),HH_SIZE varchar(255), PRIMARY KEY (HSHD_NUM))")
print("Table created successfully")
df = pd.read_csv("C:/Users/poojaReddy/OneDrive/Desktop/cloud/CloudFinalProject/dataset/Households/400_households.csv")
df.columns = df.columns.str.replace(' ', '')
df=df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
dftotuple=list(df.to_records(index=False))

for eachtuple in dftotuple:
    try:
        cursor.execute(
        '''INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)''',
           (int(eachtuple.HSHD_NUM),(eachtuple.L),(eachtuple.AGE_RANGE),eachtuple.MARITAL,eachtuple.INCOME_RANGE,eachtuple.HOMEOWNER,eachtuple.HSHD_COMPOSITION,eachtuple.HH_SIZE));
        conn.commit()
    except Exception as e:  # work on python 3.x
            print('Failed to upload to ftp: ' + str(e))
            break

conn.commit()
cursor.close()
conn.close()