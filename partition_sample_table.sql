create table dbo.Orders
(
	OrderDate smalldatetime not null,
	OrderId int not null,
	Placeholder char(100),
)
on [LOG_PartitionScheme_Small](OrderDate)
go

ALTER TABLE Orders ADD CONSTRAINT PK_Orders PRIMARY KEY Clustered (OrderDate,OrderID)
ON [LOG_PartitionScheme_Small](OrderDate)
Go

INSERT INTO [dbo].[Orders]([OrderDate],OrderId)
VALUES(DateAdd(d, ROUND(DateDiff(d, '2014-01-01', '2019-12-31') * RAND(CHECKSUM(NEWID())), 0),DATEADD(second,CHECKSUM(NEWID())%48000, '2014-01-01')),ABS(CHECKSUM(NewId())) % 1000)
GO 1000