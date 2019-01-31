USE PartitioningTest
CREATE PARTITION FUNCTION [YearlyPartitionWithCurentYearQuarterly](smalldatetime) AS RANGE RIGHT FOR VALUES (
  N'2014-01-01'
, N'2015-01-01'
, N'2016-01-01'
, N'2017-01-01'
, N'2018-01-01'
, N'2019-01-01'
, N'2019-03-01'
, N'2019-06-01'
, N'2019-09-01'
)
GO

CREATE PARTITION SCHEME [LOG_PartitionScheme_Small] AS PARTITION [YearlyPartitionWithCurentYearQuarterly] TO (
[PRIMARY]
,[LOG_2014]
, [LOG_2015]
, [LOG_2016]
, [LOG_2017]
, [LOG_2018]
, [LOG_2019]
, [LOG_2019]
, [LOG_2019]
, [LOG_2019]
)
GO
