-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "e40536ea-e4b1-b6b8-4ea5-18784c353c59",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "e40536ea-e4b1-b6b8-4ea5-18784c353c59",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "6fff9722-a63d-9366-4cdb-6ca9ca4b91e9",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

/*
drop table if EXISTS Silver_WH.dbo.silver_data
TRUNCATE TABLE silver_data

*/



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

IF EXISTS(SELECT name from sys.tables WHERE name = 'silver_data')
    BEGIN
    SELECT 'table exists' 
    END
ELSE
    BEGIN
    
    CREATE TABLE Silver_WH.dbo.silver_data (
        currency_combined VARCHAR(6) NOT NULL,
        exchange_rate DECIMAL(15,6) NOT NULL,
        ingestion_date DATE NOT NULL,
        source_date_utc DATETIME2(3) NOT NULL,
        base_currency VARCHAR(3) NOT NULL,
        target_currency VARCHAR(3) NOT NULL
    )

    ALTER TABLE Silver_WH.dbo.silver_data
    ADD CONSTRAINT Pk_CurrExchange 
    PRIMARY KEY NONCLUSTERED (currency_combined, source_date_utc) 
    NOT enforced

    SELECT 'table created'
    END

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


insert into Silver_WH.dbo.silver_data 
    select 
        currency_combined,
        exchange_rate,
        ingestion_date,
        DATEADD(second,source_date,'1970-01-01 00:00:00') as source_date_utc,
        LEFT(currency_combined,3) as base_currency, 
        RIGHT(currency_combined,3) as target_currency 
    from Bronze_WH.dbo.bronze_data 

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

select * from Silver_WH.dbo.silver_data where currency_combined = 'USDCAD' OR currency_combined = 'USDINR'
select count(*) from silver_data

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
