CREATE TABLE [dbo].[silver_data] (

	[currency_combined] varchar(6) NOT NULL, 
	[exchange_rate] decimal(15,6) NOT NULL, 
	[ingestion_date] date NOT NULL, 
	[source_date_utc] datetime2(3) NOT NULL, 
	[base_currency] varchar(3) NOT NULL, 
	[target_currency] varchar(3) NOT NULL
);


GO
ALTER TABLE [dbo].[silver_data] ADD CONSTRAINT Pk_CurrExchange primary key NONCLUSTERED ([currency_combined], [source_date_utc]);