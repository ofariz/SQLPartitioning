

CREATE DATABASE PartitioningTest
 


ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2014]
ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2015]
ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2016]
ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2017]
ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2018]
ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2019]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2014', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2014.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2014]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2015', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2015.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2015]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2016', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2016.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2016]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2017', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2017.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2017]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2018', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2018.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2018]

ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2019', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2019.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2019]

USE PartitioningTest
CREATE PARTITION FUNCTION [YearlyPartitionWithoutCurentYear](smalldatetime) AS RANGE RIGHT FOR VALUES (
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

CREATE PARTITION SCHEME [KullaniciHareket_PartitionScheme_Small] AS PARTITION [YearlyPartitionWithoutCurentYear] TO (
[LOG_2014]
, [LOG_2015]
, [LOG_2016]
, [LOG_2017]
, [LOG_2018]
, [LOG_2019]
, [LOG_2019]
, [LOG_2019]
, [LOG_2019]
,[PRIMARY]

)
GO

create table dbo.Orders
(
	OrderDate smalldatetime not null,
	OrderId int not null,
	Placeholder char(100),
)
on KullaniciHareket_PartitionScheme_Small(OrderDate)
go

ALTER TABLE Orders ADD CONSTRAINT PK_Orders PRIMARY KEY Clustered (OrderDate,OrderID)
ON KullaniciHareket_PartitionScheme_Small(OrderDate)
Go

INSERT INTO [dbo].[Orders]([OrderDate],OrderId)
VALUES(DateAdd(d, ROUND(DateDiff(d, '2014-01-01', '2019-12-31') * RAND(CHECKSUM(NEWID())), 0),DATEADD(second,CHECKSUM(NEWID())%48000, '2014-01-01')),ABS(CHECKSUM(NewId())) % 1000)
GO 1000
	 
SELECT
OBJECT_SCHEMA_NAME(pstats.object_id) AS SchemaName
,OBJECT_NAME(pstats.object_id) AS TableName
,ps.name AS PartitionSchemeName
,ds.name AS PartitionFilegroupName
,pf.name AS PartitionFunctionName
,CASE pf.boundary_value_on_right WHEN 0 THEN 'Range Left' ELSE 'Range Right' END AS PartitionFunctionRange
,CASE pf.boundary_value_on_right WHEN 0 THEN 'Upper Boundary' ELSE 'Lower Boundary' END AS PartitionBoundary
,prv.value AS PartitionBoundaryValue
,c.name AS PartitionKey
,CASE 
WHEN pf.boundary_value_on_right = 0 
THEN c.name + ' > ' + CAST(ISNULL(LAG(prv.value) OVER(PARTITION BY pstats.object_id ORDER BY pstats.object_id, pstats.partition_number), 'Infinity') AS VARCHAR(100)) + ' and ' + c.name + ' <= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100)) 
ELSE c.name + ' >= ' + CAST(ISNULL(prv.value, 'Infinity') AS VARCHAR(100)) + ' and ' + c.name + ' < ' + CAST(ISNULL(LEAD(prv.value) OVER(PARTITION BY pstats.object_id ORDER BY pstats.object_id, pstats.partition_number), 'Infinity') AS VARCHAR(100))
END AS PartitionRange
,pstats.partition_number AS PartitionNumber
,pstats.row_count AS PartitionRowCount
,sum(a.total_pages) as TotalPages, 
sum(a.used_pages) as UsedPages, 
sum(a.data_pages) as DataPages,
sum(a.total_pages) * 8 as TotalSpaceKB, 
sum(a.used_pages) * 8 as UsedSpaceKB, 
sum(a.data_pages) * 8 as DataSpaceKB
,p.data_compression_desc AS DataCompression
FROM sys.dm_db_partition_stats AS pstats
INNER JOIN sys.partitions AS p ON pstats.partition_id = p.partition_id
INNER JOIN sys.allocation_units a with (nolock) ON p.partition_id = a.container_id
INNER JOIN sys.destination_data_spaces AS dds ON pstats.partition_number = dds.destination_id
INNER JOIN sys.data_spaces AS ds ON dds.data_space_id = ds.data_space_id
INNER JOIN sys.partition_schemes AS ps ON dds.partition_scheme_id = ps.data_space_id
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.indexes AS i ON pstats.object_id = i.object_id AND pstats.index_id = i.index_id AND dds.partition_scheme_id = i.data_space_id AND i.type <= 1 /* Heap or Clustered Index */
INNER JOIN sys.index_columns AS ic ON i.index_id = ic.index_id AND i.object_id = ic.object_id AND ic.partition_ordinal > 0
INNER JOIN sys.columns AS c ON pstats.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.partition_range_values AS prv ON pf.function_id = prv.function_id AND pstats.partition_number = (CASE pf.boundary_value_on_right WHEN 0 THEN prv.boundary_id ELSE (prv.boundary_id+1) END)
GROUP BY
	pstats.object_id,
	ps.name,
	ds.name,
	pf.name,
	pf.boundary_value_on_right,
	prv.value,
	c.name,
	pstats.partition_number,
	pstats.row_count,
	p.data_compression_desc 
ORDER BY 
	TableName, PartitionNumber;
Go


------ partition splitting
--create table dbo.Orders_OLD
--(
--	OrderDate smalldatetime not null,
--	OrderId int not null,
--	Placeholder char(100),
--)
--on KullaniciHareket_PartitionScheme_Small(OrderDate)
--go

--ALTER TABLE Orders_OLD ADD CONSTRAINT PK_Orders_OLD PRIMARY KEY Clustered (OrderDate,OrderID)
--ON KullaniciHareket_PartitionScheme_Small(OrderDate)
--Go

--ALTER TABLE Orders SWITCH PARTITION 2 TO Orders_OLD PARTITION 2
--Go

 --if you want to purge truncate it. be carefull what u truncating.
 Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [PRIMARY]
  ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
MERGE RANGE ('2019-01-01')
 ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
MERGE RANGE ('2019-06-01')
 ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
MERGE RANGE ('2020-09-01')

-- partition merge
ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
MERGE RANGE ('2020-01-01')


-- partition truncate
-- partition switching


--automate partition create
--Create new Year
DECLARE @YearPartition varchar(10)='2020'


-- ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
--MERGE RANGE ('2019-03-01')
-- ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
--MERGE RANGE ('2019-06-01')
-- ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear ()  
--MERGE RANGE ('2019-09-01')

print('ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_'+@YearPartition+']
ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N''LOG_'+@YearPartition+''', FILENAME = N''C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG'+@YearPartition+'.ndf'' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_'+@YearPartition+']')
print('Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-01-01'')')

print('Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-03-01'')')

print('Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-06-01'')')

print('Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-09-01'')')

 
 ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_2020]
ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N'LOG_2020', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG2020.ndf' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_2020]
Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_2020]
ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'2020-01-01')
Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_2020]
ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'2020-03-01')
Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_2020]
ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'2020-06-01')
Alter Partition Scheme KullaniciHareket_PartitionScheme_Small NEXT USED [LOG_2020]
ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'2020-09-01')




 
Declare @DtOldestBoundary AS datetime
Declare @strFileGroupToBeUsed AS VARCHAR(100)
Declare @PartitionNumber As int
SELECT @strFileGroupToBeUsed = fg.name, @PartitionNumber = p.partition_number, @DtOldestBoundary = cast(prv.value as datetime) FROM sys.partitions p 
INNER JOIN sys.sysobjects tab on tab.id = p.object_id
INNER JOIN sys.allocation_units au ON au.container_id = p.hobt_id 
INNER JOIN sys.filegroups fg ON fg.data_space_id = au.data_space_id 
INNER JOIN SYS.partition_range_values prv ON prv.boundary_id = p.partition_number
INNER JOIN sys.partition_functions PF ON pf.function_id = prv.function_id
WHERE 1=1
AND pf.name = 'YearlyPartitionWithoutCurentYear'
AND tab.name = 'Orders'
AND cast(value as datetime) = (
SELECT MIN(cast(value as datetime)) FROM sys.partitions p 
INNER JOIN sys.sysobjects tab on tab.id = p.object_id
INNER JOIN SYS.partition_range_values prv ON prv.boundary_id = p.partition_number
INNER JOIN sys.partition_functions PF ON pf.function_id = prv.function_id
WHERE 1=1
AND pf.name = 'YearlyPartitionWithoutCurentYear'
AND tab.name = 'Orders'
)
Select @DtOldestBoundary Oldest_Boundary , @strFileGroupToBeUsed FileGroupToBeUsed,@PartitionNumber PartitionNumber
ALTER TABLE Orders SWITCH PARTITION @PartitionNumber TO Orders_Work PARTITION @PartitionNumber
TRUNCATE TABLE Orders_Work
EXEC('Alter Partition Scheme OrderPartitionScheme NEXT USED '+@strFileGroupToBeUsed)
Alter Partition Function OrderPartitionFunction() SPLIT RANGE (@DtNextBoundary);
Alter Partition Function OrderPartitionFunction() MERGE RANGE (@DtOldestBoundary);





declare @f float = 123456.789;

select
  [raw]      = str(@f,20,3)
 ,[standard] = cast(format(@f, 'N', 'en-US') as varchar(20))
 ,[European] = cast(format(@f, 'N', 'de-de') as varchar(20))