# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "56965f47-f439-450d-a6af-3f0d39d1d003",
# META       "default_lakehouse_name": "LH",
# META       "default_lakehouse_workspace_id": "d7e270b0-4c3f-4eba-b135-621b83ad5c54",
# META       "known_lakehouses": [
# META         {
# META           "id": "56965f47-f439-450d-a6af-3f0d39d1d003"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyodbc
from pyspark.sql.functions import col, desc, row_number
from pyspark.sql.window import Window
#from pyspark.sql import functions as f


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet("Files/creds")
password = df2.collect()[0]["password"]

jdbc_url = "jdbc:sqlserver://fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com:\
            1433;databaseName=sql_project;encrypt=true;trustServerCertificate=true"

jdbc_properties = {
    "user": "admin",
    "password": password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

conn_str_master = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE=master;"
            f"UID=admin;"
            f"PWD={password};"
            f"Encrypt=yes;"
            f"TrustServerCertificate=yes;"
            f"Connect Timeout=30;"
        )
        
conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER=fabric-rds-sql-server.cxm8ga0awaka.eu-north-1.rds.amazonaws.com,1433;"
            f"DATABASE=sql_project;"
            f"UID=admin;"
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
                                EXEC('CREATE DATABASE ' + ?)
                                SELECT '' + ? + ' created'
                                END

            ""","sql_project","sql_project","sql_project","sql_project","sql_project")
            
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
w = Window.partitionBy(col("currency_combined")).orderBy(col("source_date_utc"))

df = df.withColumn("rn", row_number().over(w)).filter(col("rn")==1).drop("rn")


try:
        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", jdbc_properties["driver"]) \
            .option("batchsize", 1000) \
            .mode("append") \
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

# CELL ********************

#TESTING...

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
