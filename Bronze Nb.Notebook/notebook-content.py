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
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

import requests, json, com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pprint import pprint
from pyspark.sql.functions import col, length, substring, lit
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, DateType, IntegerType
from datetime import datetime, date

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_api = spark.read.parquet("Files/creds")
api_key = df_api.collect()[0]['api']

schema = StructType([
    StructField("currency_combined",StringType(), False),
    StructField("exchange_rate",DoubleType(), False),
    StructField("ingestion_date",DateType(), False),
    StructField("source_date",IntegerType(), False),
])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    url = f"https://api.exchangerate.host/live?access_key={api_key}"
    response = requests.get(url)

except Exception as e:
    print(e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

data = response.json()['quotes']
timestamp = response.json()['timestamp']

data_list = [(k,float(v),date.today(),timestamp) for k,v in data.items()]


df = spark.createDataFrame(data_list,schema=schema)
print(df.count())
display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode("overwrite").synapsesql("Bronze_WH.dbo.bronze_data")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
