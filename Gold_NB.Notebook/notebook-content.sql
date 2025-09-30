-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "warehouse": {
-- META       "default_warehouse": "2ebe39ba-663c-8c6c-44e2-10d6c1ce850a",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "c3fa419b-3781-abb9-4e95-5b5dbc13e465",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "e40536ea-e4b1-b6b8-4ea5-18784c353c59",
-- META           "type": "Datawarehouse"
-- META         },
-- META         {
-- META           "id": "2ebe39ba-663c-8c6c-44e2-10d6c1ce850a",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- MARKDOWN ********************

-- Table creation

-- CELL ********************

if EXISTS(select name from sys.tables where name = 'gold_data')
    BEGIN
    select 'table exists'
    select * from gold_data
    END
ELSE
    BEGIN
    select 'doesnt exist'
    CREATE TABLE GOLD_WH.dbo.gold_data (
        currency_combined VARCHAR(6) NOT NULL,
        exchange_rate DECIMAL(20, 9) NOT NULL,  
        ingestion_date DATE NOT NULL,
        source_date_utc DATETIME2(3) NOT NULL,
        base_currency VARCHAR(3) NOT NULL,
        target_currency VARCHAR(3) NOT NULL,
        start_date DATETIME2(3) NOT NULL,
        end_date DATETIME2(3),
        is_current BIT NOT NULL
    );
    alter table Gold_WH.dbo.gold_data
    add CONSTRAINT PK_CurrExchange 
    PRIMARY KEY NONCLUSTERED (currency_combined,source_date_utc) 
    NOT enforced
    SELECT 'table created'
    END

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- Silver view

-- CELL ********************

/*
DROP VIEW IF EXISTS silver
GO

CREATE VIEW silver AS
SELECT *
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY currency_combined ORDER BY source_date_utc DESC) AS rn
    FROM Silver_WH.dbo.silver_data s
) t
WHERE rn = 1;
*/



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- MARKDOWN ********************

-- SCD2

-- CELL ********************


INSERT INTO gold_data (
    currency_combined, exchange_rate, ingestion_date,
    source_date_utc, base_currency, target_currency,
    start_date, end_date, is_current
)
SELECT 
    s.currency_combined,
    s.exchange_rate,
    s.ingestion_date,
    s.source_date_utc,
    s.base_currency,
    s.target_currency,
    GETDATE(),   -- new version starts now
    NULL,
    1
FROM silver s
INNER JOIN gold_data g
    ON s.currency_combined = g.currency_combined
WHERE g.end_date IS NULL
  AND g.exchange_rate <> s.exchange_rate;   




UPDATE g
SET 
    g.end_date = GETDATE(),
    g.is_current = 0
FROM gold_data g
INNER JOIN silver s
    ON g.currency_combined = s.currency_combined
WHERE g.is_current = 1
  AND g.exchange_rate <> s.exchange_rate;


  

  
INSERT INTO gold_data (
    currency_combined, exchange_rate, ingestion_date,
    source_date_utc, base_currency, target_currency,
    start_date, end_date, is_current
)
SELECT 
    s.currency_combined,
    s.exchange_rate,
    s.ingestion_date,
    s.source_date_utc,
    s.base_currency,
    s.target_currency,
    s.source_date_utc,   -- first version starts at source date
    NULL,
    1
FROM silver s
LEFT JOIN gold_data g
    ON s.currency_combined = g.currency_combined
WHERE g.currency_combined IS NULL;

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

/*
TESTING....


select * from silver_WH.dbo.silver_data order by source_date_utc asc
select * from Gold_WH.dbo.gold_data order by source_date_utc asc 
select count(*) from Gold_WH.dbo.gold_data where is_current = 1

SELECT *
FROM gold_data
WHERE is_current = 1

SELECT currency_combined, COUNT(*) AS cnt
FROM gold_data
WHERE is_current = 1
GROUP BY currency_combined
HAVING COUNT(*) > 1;


*/



-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

select * from silver_WH.dbo.silver_data order by source_date_utc asc

-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************

select * from Gold_WH.dbo.gold_data 
order by is_current DESC


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }

-- CELL ********************


-- METADATA ********************

-- META {
-- META   "language": "sql",
-- META   "language_group": "sqldatawarehouse"
-- META }
