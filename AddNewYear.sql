DECLARE @YearPartition varchar(10)='2020'
DECLARE @DBNAME varchar(50) ='DBNAME'
DECLARE @SCHEMA_NAME varchar(50) ='SCHEMA_NAME'
DECLARE @PARTITION_FUNCTION_NAME varchar(50) ='PARTITION_FUNCTION_NAME'

print('USE [master]
GO
ALTER DATABASE ['+@DBNAME+'] ADD FILEGROUP [Part_'+@YearPartition+']
ALTER DATABASE ['+@DBNAME+'] ADD FILE ( NAME = N''Part_'+@YearPartition+''', FILENAME = N''D:\MSSQL\MSSQL\DATA\'+@DBNAME+'_PART_'+@YearPartition+'.ndf'' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [Part_'+@YearPartition+']')

print('USE ['+@DBNAME+']
GO')
print('Alter Partition Scheme ['+@SCHEMA_NAME+'] NEXT USED [Part_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION '+@PARTITION_FUNCTION_NAME+'() 
SPLIT RANGE (N'''+@YearPartition+'-31-01'')')
