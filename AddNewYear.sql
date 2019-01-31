DECLARE @YearPartition varchar(10)='2020'


print('ALTER DATABASE [PartitioningTest] ADD FILEGROUP [LOG_'+@YearPartition+']
ALTER DATABASE [PartitioningTest] ADD FILE ( NAME = N''LOG_'+@YearPartition+''', FILENAME = N''C:\Program Files\Microsoft SQL Server\MSSQL13.MSSQLSERVER\MSSQL\DATA\PartitioningTest_LOG'+@YearPartition+'.ndf'' , SIZE = 10MB , FILEGROWTH = 10MB ) TO FILEGROUP [LOG_'+@YearPartition+']')
print('Alter Partition Scheme LOG_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-01-01'')')

print('Alter Partition Scheme LOG_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-03-01'')')

print('Alter Partition Scheme LOG_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-06-01'')')

print('Alter Partition Scheme LOG_PartitionScheme_Small NEXT USED [LOG_'+@YearPartition+']')
print('ALTER PARTITION FUNCTION YearlyPartitionWithoutCurentYear()  
SPLIT RANGE (N'''+@YearPartition+'-09-01'')')
