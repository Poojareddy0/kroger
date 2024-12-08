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

cursor.execute("DROP TABLE IF EXISTS PRODUCTS;")
print("Finished dropping table (if existed).")

cursor.execute("CREATE TABLE PRODUCTS (PRODUCT_NUM int,DEPARTMENT varchar(255),COMMODITY varchar(255),BRAND_TY varchar(255),NATURAL_ORGANIC_FLAG varchar(255),PRIMARY KEY (PRODUCT_NUM))")
print("Table created successfully")
df = pd.read_csv("C:/Users/poojaReddy/OneDrive/Desktop/cloud/CloudFinalProject/dataset/Products/400_products.csv")
df.columns = df.columns.str.replace(' ', '')
df=df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
dftotuple=list(df.to_records(index=False))

for eachtuple in dftotuple:
    try:
        cursor.execute(
        '''INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s,%s,%s,%s,%s)''',
         (int(eachtuple.PRODUCT_NUM),(eachtuple.DEPARTMENT),(eachtuple.COMMODITY),(eachtuple.BRAND_TY),(eachtuple.NATURAL_ORGANIC_FLAG)));
        conn.commit()
    except Exception as e:  # work on python 3.x
            print('Failed to upload to ftp: ' + str(e))
            break

conn.commit()
cursor.close()
conn.close()