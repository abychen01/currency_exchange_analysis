CREATE TABLE [dbo].[bronze_data] (

	[currency_combined] varchar(6) NOT NULL, 
	[exchange_rate] float NOT NULL, 
	[ingestion_date] date NOT NULL, 
	[source_date] int NOT NULL
);