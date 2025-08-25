CREATE TABLE [dbo].[gold_data] (

	[currency_combined] varchar(6) NOT NULL, 
	[exchange_rate] decimal(10,5) NOT NULL, 
	[ingestion_date] date NOT NULL, 
	[source_date_utc] datetime2(3) NOT NULL, 
	[base_currency] varchar(3) NOT NULL, 
	[target_currency] varchar(3) NOT NULL, 
	[start_date] datetime2(3) NOT NULL, 
	[end_date] datetime2(3) NULL, 
	[is_current] bit NOT NULL
);


GO
ALTER TABLE [dbo].[gold_data] ADD CONSTRAINT PK_CurrExchange primary key NONCLUSTERED ([currency_combined], [source_date_utc]);