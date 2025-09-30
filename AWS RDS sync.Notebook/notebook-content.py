# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1d649236-807c-4714-b503-a91b05ffe731",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "886e004c-d9e1-4ff5-9e4c-e4e00f921dba",
# META       "known_lakehouses": [
# META         {
# META           "id": "1d649236-807c-4714-b503-a91b05ffe731"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Imports

# CELL ********************

import pyodbc
from pyspark.sql.functions import col, desc, row_number, asc
from pyspark.sql.window import Window
#from pyspark.sql import functions as f


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Declarations

# CELL ********************

df2 = spark.read.parquet("Files/creds")
password = df2.collect()[0]["password"]

db = "currency_exchange_analysis"

jdbc_url = f"jdbc:sqlserver://myfreesqldbserver66.database.windows.net:1433;" \
           f"databaseName={db};" \
           "encrypt=true;" \
           "trustServerCertificate=false;" \
           "hostNameInCertificate=*.database.windows.net;" \
           "loginTimeout=30;"

jdbc_properties = {
    "user": "admin2",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE=master;"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=tcp:myfreesqldbserver66.database.windows.net,1433;"
            f"DATABASE={db};"
            f"UID=admin2;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### DB check

# CELL ********************

try:
    with pyodbc.connect(conn_str_master,autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                            IF EXISTS (SELECT name FROM sys.databases WHERE name = ?)
                                BEGIN
                                SELECT ? + ' exists'
                                END
                            ELSE
                                BEGIN
                                SELECT '' + ? + ' doesnt exist'
                                END

            """,db,db,db)
            
            while True:
                result = cursor.fetchone()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break

except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Table check/create

# CELL ********************

table = "currency_exchange_data"

try:
    with pyodbc.connect(conn_str,autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute(""" 
                            IF EXISTS (SELECT name FROM sys.tables WHERE name = ?)
                                BEGIN
                                SELECT '[' + ? + '] exists'
                                EXEC('SELECT COUNT(*) FROM ' + ?)
                                END
                            ELSE
                                BEGIN
                                SELECT '[' + ? + '] does not exist'
                                EXEC('CREATE TABLE ' + ? + '(
                                        currency_combined VARCHAR(6) NOT NULL,
                                        exchange_rate DECIMAL(10, 5) NOT NULL,  
                                        ingestion_date DATE NOT NULL,
                                        source_date_utc DATETIME2(3) NOT NULL,
                                        base_currency VARCHAR(3) NOT NULL,
                                        target_currency VARCHAR(3) NOT NULL,
                                        start_date DATETIME2(3) NOT NULL,
                                        end_date DATETIME2(3),
                                        is_current BIT NOT NULL
                                    )'
                                )
                                SELECT '[' + ? + '] created'
                                END

            """,table,table,table,table,table, table )
            
            while True:
                result = cursor.fetchone()
                if result:
                    print(result[0])
                if not cursor.nextset():
                    break

except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************



df = spark.read.table("gold_data")

try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("overwrite") \
            .save()
        print(f"Successfully wrote data to RDS table [{table}].")

except Exception as e:
    print(f"Failed to write to RDS: {e}")
    raise



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Testing

# CELL ********************

#TESTING....

'''


try:
    with pyodbc.connect(conn_str,autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("drop table currency_exchange_data")
            
            result = cursor.fetchall()
            print(result)
except Exception as e:
    print(e)


'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
w = Window.partitionBy(col("currency_combined")).orderBy(desc(col("source_date_utc")))
display(w)
df = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1)
display(df)
#display(df.where(col("is_current")==True))
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
