# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2aa9bac7-1328-454f-bdb3-eaf2f536c9a4",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "2410a0ef-6484-4131-aad4-3c70feee320e",
# META       "known_lakehouses": [
# META         {
# META           "id": "2aa9bac7-1328-454f-bdb3-eaf2f536c9a4"
# META         }
# META       ]
# META     },
# META     "warehouse": {}
# META   }
# META }

# MARKDOWN ********************

# #### Imports

# CELL ********************

import requests, json, com.microsoft.spark.fabric, os
from com.microsoft.spark.fabric.Constants import Constants
from pprint import pprint
from pyspark.sql.functions import col, length, substring, lit
from pyspark.sql.types import StructField, StructType, DoubleType, StringType, DateType, IntegerType
from datetime import datetime, date, timedelta

from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Defs

# CELL ********************

df_creds = spark.read.parquet("Files/creds")

os.environ["AZURE_CLIENT_ID"] = df_creds.collect()[0]["AZURE_CLIENT_ID"]
os.environ["AZURE_TENANT_ID"] = df_creds.collect()[0]["AZURE_TENANT_ID"]
os.environ["AZURE_CLIENT_SECRET"] = df_creds.collect()[0]["AZURE_CLIENT_SECRET"]

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

# MARKDOWN ********************

# #### Vault call

# CELL ********************

vault_url = "https://vaultforfabric.vault.azure.net/"
credential = DefaultAzureCredential()
client = SecretClient(vault_url=vault_url, credential=credential)

api_key = client.get_secret("exhange-rate-host-api").value

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### API call

# CELL ********************

try:
    url = f"https://api.exchangerate.host/live?access_key={api_key}"
    response = requests.get(url)

except Exception as e:
    print(e)



data = response.json()['quotes']                # rates data 
timestamp = response.json()['timestamp']        # timestamp of response

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
