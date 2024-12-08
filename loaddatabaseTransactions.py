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

cursor.execute("DROP TABLE IF EXISTS TRANSACTIONS;")
print("Finished dropping table (if existed).")

cursor.execute("CREATE TABLE TRANSACTIONS (BASKET_NUM INT,HSHD_NUM INT,PURCHASE_ VARCHAR(255),PRODUCT_NUM INT,SPEND INT,UNITS INT,STORE_R VARCHAR(255),WEEK_NUM INT,YEAR INT)")
print("Table created successfully")
df = pd.read_csv("C:/Users/poojaReddy/OneDrive/Desktop/cloud/CloudFinalProject/dataset/Transactions/400_transactions.csv")
df.columns = df.columns.str.replace(' ', '')
df=df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
dftotuple=list(df.to_records(index=False))

for eachtuple in dftotuple:
    try:
        cursor.execute(
        '''INSERT INTO transactions (BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (int(eachtuple.BASKET_NUM),int(eachtuple.HSHD_NUM),str(eachtuple.PURCHASE_),int(eachtuple.PRODUCT_NUM),int(eachtuple.SPEND),int(eachtuple.UNITS),str(eachtuple.STORE_R),int(eachtuple.WEEK_NUM),int(eachtuple.YEAR)));
        conn.commit()
    except Exception as e:  # work on python 3.x
            print('Failed to upload to ftp: ' + str(e))
            break

conn.commit()
cursor.close()
conn.close()