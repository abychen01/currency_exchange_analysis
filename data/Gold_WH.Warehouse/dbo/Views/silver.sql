-- Auto Generated (Do not modify) 2D1DD27EB26E894DE81614D8849BAB4C273C21C2A46240DA059B236AFBD5A24B


CREATE VIEW silver AS
SELECT *
FROM (
    SELECT s.*,
           ROW_NUMBER() OVER (PARTITION BY currency_combined ORDER BY source_date_utc DESC) AS rn
    FROM Silver_WH.dbo.silver_data s
) t
WHERE rn = 1;