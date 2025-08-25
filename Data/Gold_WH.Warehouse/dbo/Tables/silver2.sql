CREATE TABLE [dbo].[silver2] (

	[currency_combined] varchar(20) NOT NULL, 
	[exchange_rate] float NOT NULL, 
	[ingestion_date] date NOT NULL, 
	[source_date_utc] date NOT NULL, 
	[base_currency] varchar(10) NOT NULL, 
	[target_currency] varchar(10) NOT NULL, 
	[rn] int NOT NULL
);