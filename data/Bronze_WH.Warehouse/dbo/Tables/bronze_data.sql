CREATE TABLE [dbo].[bronze_data] (

	[currency_combined] varchar(max) NOT NULL, 
	[exchange_rate] float NOT NULL, 
	[ingestion_date] date NOT NULL, 
	[source_date] int NOT NULL
);