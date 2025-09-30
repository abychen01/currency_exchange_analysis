# Currency Exchange Analysis 

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repo-blue?logo=github)](https://github.com/abychen01/currency_exchange_analysis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/Python-3.8%2B-blue)](https://www.python.org/downloads/)
[![Spark Version](https://img.shields.io/badge/Spark-3.x-orange)](https://spark.apache.org/)

## Overview
This repository contains an ETL (Extract, Transform, Load) pipeline built using Microsoft Fabric (Synapse Analytics) for fetching, processing, and analyzing daily currency exchange rates. The pipeline uses a layered architecture (Bronze, Silver, Gold) to handle raw data ingestion, transformations, and Slowly Changing Dimension Type 2 (SCD2) for historical tracking. Data is sourced from a free API (e.g., exchangerate.host), processed with PySpark and SQL, stored in Fabric's data warehouse, and synced to an Azure SQL Server for further analysis or visualization in Power BI.

Key Features:
- **Daily Updates**: Fetches live exchange rates for 170+ currencies (USD-based).
- **Data Layers**: Bronze (raw), Silver (cleaned/current), Gold (historical with SCD2).
- **Sync to SQL Server**: Exports final data to Azure SQL Server for external access.
- **Pipeline Orchestration**: Sequential execution via Fabric pipelines (Bronze → Silver → Gold → Sync).
- **Use Cases**: Currency trend analysis, Power BI dashboards (e.g., exchange rate vs. time, inflation correlations).

This setup is ideal for building Power BI visuals on economic data, with scalability for adding more APIs (e.g., World Bank for inflation/GDP).

## Architecture
The pipeline follows a medallion architecture:
1. **Bronze Layer**: Raw data ingestion.
2. **Silver Layer**: Data cleaning and deduplication (latest records).
3. **Gold Layer**: Historical versioning with SCD2.
4. **Sync**: Export to external SQL Server.

Data flows through Fabric's Lakehouse/Warehouse, using PySpark for ETL and SQL for transformations.

<img width="1414" alt="image" src="https://github.com/abychen01/currency_exchange_analysis/blob/99ad96f47872f0dea63752c2a36bd411802283af/CE%20Pipeline.png" />

## Notebooks
The repository includes four Jupyter notebooks (`.ipynb`) that form the core of the pipeline. Each handles a specific stage and is executed sequentially in Microsoft Fabric.

### 1. Bronze_NB.ipynb
- **Purpose**: Ingests raw currency exchange data from the API.
- **Key Operations**:
  - Imports libraries: `requests`, `json`, `pprint`, PySpark functions/types, `datetime`.
  - Reads API key from a Parquet file (`Files/creds`).
  - Defines a schema for the DataFrame (currency_combined, exchange_rate, ingestion_date, source_date).
  - Fetches live data from `https://api.exchangerate.host/live` using the API key.
  - Parses JSON response, creates a list of tuples with today's date and timestamp.
  - Creates a PySpark DataFrame and writes it (overwrite mode) to the Bronze warehouse table (`Bronze_WH.dbo.bronze_data`).
- **Output**: Raw table with ~171 rows (one per currency pair).
- **Dependencies**: API key in Parquet; runs daily for fresh data.

### 2. Silver_NB.ipynb
- **Purpose**: Transforms and cleans Bronze data, storing the latest records.
- **Key Operations**:
  - Checks/creates the Silver table (`Silver_WH.dbo.silver_data`) with columns: currency_combined, exchange_rate, ingestion_date, source_date_utc, base_currency, target_currency.
  - Adds a primary key constraint (non-enforced).
  - Inserts data from Bronze: Converts UNIX timestamp to UTC datetime, splits currency_combined into base/target currencies.
  - Truncates or drops table if needed.
  - Queries for validation (e.g., count, specific pairs like USDCAD/USDINR).
- **Output**: Cleaned table with latest exchange rates.
- **Dependencies**: Runs after Bronze; uses SQL for inserts.

### 3. Gold_NB.ipynb
- **Purpose**: Applies SCD2 for historical tracking of changes in exchange rates.
- **Key Operations**:
  - Checks/creates the Gold table (`GOLD_WH.dbo.gold_data`) with additional columns: start_date, end_date, is_current.
  - Creates a view (`silver`) to get the latest records from Silver (using ROW_NUMBER() for deduplication).
  - Inserts new versions into Gold when rates change: Sets start_date to current time, end_date NULL, is_current=1.
  - Updates existing records: Sets end_date to current time, is_current=0 for outdated versions.
  - Handles truncated data in queries (e.g., sample output shows historical records with end dates).
- **Output**: Versioned table for time-based analysis (e.g., rate changes over time).
- **Dependencies**: Runs after Silver; implements change detection via joins.

### 4. SQL_Server_sync.ipynb
- **Purpose**: Syncs the final Gold data to an external Azure SQL Server (RDS) for broader access.
- **Key Operations**:
  - Imports: `pyodbc`, PySpark functions, Window.
  - Reads credentials from Parquet (`Files/creds`).
  - Defines JDBC URL and properties for Azure SQL connection.
  - Checks if database (`currency_exchange_analysis`) exists.
  - Checks/creates RDS table (`currency_exchange_data`) with matching schema.
  - Reads from Gold table and writes to SQL Server via JDBC in batches of 1000.
- **Output**: Synced table in Azure SQL for querying outside Fabric.
- **Dependencies**: Runs after Gold; requires ODBC driver and credentials.

## Pipeline Setup
The notebooks are orchestrated in a Microsoft Fabric pipeline for sequential execution:
1. **Bronze_NB** → Fetches raw data.
2. **Silver_NB** → Cleans and loads latest data.
3. **Gold_NB** → Applies SCD2 for history.
4. **SQL_Server_sync** → Exports to RDS.

- **Scheduling**: Set to run Monday to Friday via Fabric's scheduler.
- **Error Handling**: Basic try-except in notebooks; add alerts in Fabric for production.


## Prerequisites
- **Microsoft Fabric**: Workspace with Lakehouse and Warehouse enabled.
- **Azure SQL Server**: Free tier or equivalent (e.g., `myfreesqldbserver66.database.windows.net`).
- **API Key**: From exchangerate.host; stored in `Files/creds` Parquet.
- **Dependencies**:
  - Python: `requests`, `pyodbc`, `pprint`, `datetime`.
  - PySpark: Built-in in Fabric.
  - ODBC Driver: Version 18 for SQL Server.
- **Setup**:
  1. Clone repo: `git clone https://github.com/abychen01/currency_exchange_analysis.git`.
  2. Upload notebooks to Fabric workspace.
  3. Create pipeline and add notebook activities in order.
  4. Store credentials securely (e.g., in Fabric's Key Vault or Parquet).


## Contributing
- Fork the repo and submit pull requests for improvements (e.g., more currencies, error logging).
- Issues: Report bugs or suggest features via GitHub Issues.


## Contact
- Repo Owner: [abychen01](https://github.com/abychen01)
- For questions: Open an issue.

*Last Updated: September 30, 2025*
