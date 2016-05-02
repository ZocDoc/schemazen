USE [Maintenance]
GO

/****** Object:  UserDefinedFunction [dbo].[CrontabLastOccurrence]    Script Date: 4/29/2016 12:36:15 PM ******/
SET ANSI_NULLS OFF
GO

SET QUOTED_IDENTIFIER OFF
GO

CREATE FUNCTION [dbo].[CrontabLastOccurrence5](@Expression [nvarchar](100), @Start [datetime], @End [datetime])
RETURNS  TABLE (
	[Occurrence] [datetime] NULL
) WITH EXECUTE AS owner
AS 
EXTERNAL NAME [Zocron.SqlServer].[Zocron.SqlServer.Functions.Crontab].[GetLastOccurrence]
GO



select *
from INFORMATION_SCHEMA.parameters
where SPECIFIC_NAME like 'CrontabLastOccurrence5'


select 
	ob.object_id
,	ob.name
,	sc.name as schema_name
,	ob.type
,	ob.type_desc
,	ass.name as assembly_name
,	am.assembly_class
,	am.assembly_method
,	am.execute_as_principal_id
,	case
		when execute_as_principal_id = -2 then 'OWNER' 
		when execute_as_principal_id = 1 then 'SELF' 
		when execute_as_principal_id IS NULL then 'CALLER' 
		else pr.name 
	END as execute_as
,	params.parameter_string
from sys.objects ob
inner join sys.schemas sc
	on ob.schema_id = sc.schema_id
inner join sys.assembly_modules am
	on ob.object_id = am.object_id
inner join sys.assemblies ass
	on ass.assembly_id = am.assembly_id
left outer join sys.database_principals pr
	on pr.principal_id = am.execute_as_principal_id
outer apply (
	SELECT 
	  STUFF((
		SELECT ', ' + [Parameter_Name] + ' ' + Data_type + case when character_maximum_length is not null then '(' + cast(character_maximum_length as nvarchar) + ')' else '' end
		FROM INFORMATION_SCHEMA.parameters
		WHERE (SPECIFIC_NAME = Results.SPECIFIC_NAME) 
		FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')
	  ,1,2,'') AS parameter_string
	FROM INFORMATION_SCHEMA.parameters Results
	where SPECIFIC_NAME = ob.name
	GROUP BY SPECIFIC_NAME
) params




select *
from sys.types

select *
from sys.parameters
where object_id = 1099150961

select *
from INFORMATION_SCHEMA.routines
where SPECIFIC_NAME like 'CrontabLastOccurrence2'

SELECT 
  STUFF((
    SELECT ', ' + [Parameter_Name] + ' ' + Data_type + case when character_maximum_length is not null then '(' + cast(character_maximum_length as nvarchar) + ')' else '' end
    FROM INFORMATION_SCHEMA.parameters
    WHERE (SPECIFIC_NAME = Results.SPECIFIC_NAME) 
    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')
  ,1,2,'') AS parameter_string
FROM INFORMATION_SCHEMA.parameters Results
where SPECIFIC_NAME like 'CrontabLastOccurrence5'
GROUP BY SPECIFIC_NAME




SELECT 
  object_id,
  STUFF((
    SELECT ', ' + [Name]
    FROM sys.parameters 
    WHERE (object_id = Results.object_id) 
    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')
  ,1,2,'') AS NameValues
FROM sys.parameters Results
where object_id = 1099150961
GROUP BY object_id


select *
from sys.columns
where object_id = 1099150961


SELECT *
FROM sys.objects
WHERE type_desc LIKE '%FUNCTION%'

select * from sys.assembly_modules

select *
from sys.parameters
where object_id = 1733581214



select * from sys.schemas
from sys.objects
where object_id = 1733581214

drop table #views

select 
	ob.name
,	0 as processed
into #views
from sys.all_columns co
inner join sys.all_objects ob
	on co.object_id = ob.object_id
where co.name like 'object_id' and type = 'V'
order by ob.name 

declare @current nvarchar(255) = (select top 1 name from #views)

while (@current is not null)
begin

	declare @sql nvarchar(max) = 'select ''' + @current + ''' as view_name, * from sys.' + @current + ' where object_id = 1733581214'

	exec(@sql)

	update #views set processed = 1 where name = @current

	set @current = (select top 1 name from #views where processed = 0)
end