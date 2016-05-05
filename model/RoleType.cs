using System.Collections.Generic;
using System.Data.SqlClient;

namespace SchemaZen.model {
    public class RoleType : DatabaseComponentBase<Role> {
        public override string Name {
            get { return "roles"; }
        }

        public override List<Role> Load(SqlCommand cm) {
            var roles = new List<Role>();

            //Roles are complicated.  This was adapted from https://dbaeyes.wordpress.com/2013/04/19/fully-script-out-a-mssql-database-role/
            cm.CommandText = @"
create table #ScriptedRoles (
	name nvarchar(255) not null
,	script nvarchar(max)
)

insert into #ScriptedRoles
select 
	name
,	null as script 
from sys.database_principals
where type = 'R'
	and name not in (
	-- Ignore default roles, just look for custom ones
		'db_accessadmin'
	,	'db_backupoperator'
	,	'db_datareader'
	,	'db_datawriter'
	,	'db_ddladmin'
	,	'db_denydatareader'
	,	'db_denydatawriter'
	,	'db_owner'
	,	'db_securityadmin'
	,	'public'
	)

while(exists(select 1 from #ScriptedRoles where script is null))
begin

	DECLARE @RoleName VARCHAR(255)
	SET @RoleName = (select top 1 name from #ScriptedRoles where script is null)

	-- Script out the Role
	DECLARE @roleDesc VARCHAR(MAX), @crlf VARCHAR(2)
	SET @crlf = CHAR(13) + CHAR(10)
	SET @roleDesc = 'CREATE ROLE [' + @roleName + ']' + @crlf + 'GO' + @crlf + @crlf

	SELECT    @roleDesc = @roleDesc +
			CASE dp.state
				WHEN 'D' THEN 'DENY '
				WHEN 'G' THEN 'GRANT '
				WHEN 'R' THEN 'REVOKE '
				WHEN 'W' THEN 'GRANT '
			END + 
			dp.permission_name + ' ' +
			CASE dp.class
				WHEN 0 THEN ''
				WHEN 1 THEN --table or column subset on the table
					CASE WHEN dp.major_id < 0 THEN
						+ 'ON [sys].[' + OBJECT_NAME(dp.major_id) + '] '
					ELSE
						+ 'ON [' +
						(SELECT SCHEMA_NAME(schema_id) + '].[' + name FROM sys.objects WHERE object_id = dp.major_id)
							+ -- optionally concatenate column names
						CASE WHEN MAX(dp.minor_id) > 0 
							 THEN '] ([' + REPLACE(
											(SELECT name + '], [' 
											 FROM sys.columns 
											 WHERE object_id = dp.major_id 
												AND column_id IN (SELECT minor_id 
																  FROM sys.database_permissions 
																  WHERE major_id = dp.major_id
																	AND USER_NAME(grantee_principal_id) IN (@roleName)
																 )
											 FOR XML PATH('')
											) --replace final square bracket pair
										+ '])', ', []', '')
							 ELSE ']'
						END + ' '
					END
				WHEN 3 THEN 'ON SCHEMA::[' + SCHEMA_NAME(dp.major_id) + '] '
				WHEN 4 THEN 'ON ' + (SELECT RIGHT(type_desc, 4) + '::[' + name FROM sys.database_principals WHERE principal_id = dp.major_id) + '] '
				WHEN 5 THEN 'ON ASSEMBLY::[' + (SELECT name FROM sys.assemblies WHERE assembly_id = dp.major_id) + '] '
				WHEN 6 THEN 'ON TYPE::[' + (SELECT name FROM sys.types WHERE user_type_id = dp.major_id) + '] '
				WHEN 10 THEN 'ON XML SCHEMA COLLECTION::[' + (SELECT SCHEMA_NAME(schema_id) + '.' + name FROM sys.xml_schema_collections WHERE xml_collection_id = dp.major_id) + '] '
				WHEN 15 THEN 'ON MESSAGE TYPE::[' + (SELECT name FROM sys.service_message_types WHERE message_type_id = dp.major_id) + '] '
				WHEN 16 THEN 'ON CONTRACT::[' + (SELECT name FROM sys.service_contracts WHERE service_contract_id = dp.major_id) + '] '
				WHEN 17 THEN 'ON SERVICE::[' + (SELECT name FROM sys.services WHERE service_id = dp.major_id) + '] '
				WHEN 18 THEN 'ON REMOTE SERVICE BINDING::[' + (SELECT name FROM sys.remote_service_bindings WHERE remote_service_binding_id = dp.major_id) + '] '
				WHEN 19 THEN 'ON ROUTE::[' + (SELECT name FROM sys.routes WHERE route_id = dp.major_id) + '] '
				WHEN 23 THEN 'ON FULLTEXT CATALOG::[' + (SELECT name FROM sys.fulltext_catalogs WHERE fulltext_catalog_id = dp.major_id) + '] '
				WHEN 24 THEN 'ON SYMMETRIC KEY::[' + (SELECT name FROM sys.symmetric_keys WHERE symmetric_key_id = dp.major_id) + '] '
				WHEN 25 THEN 'ON CERTIFICATE::[' + (SELECT name FROM sys.certificates WHERE certificate_id = dp.major_id) + '] '
				WHEN 26 THEN 'ON ASYMMETRIC KEY::[' + (SELECT name FROM sys.asymmetric_keys WHERE asymmetric_key_id = dp.major_id) + '] '
			 END COLLATE SQL_Latin1_General_CP1_CI_AS
			 + 'TO [' + @roleName + ']' + 
			 CASE dp.state WHEN 'W' THEN ' WITH GRANT OPTION' ELSE '' END + @crlf
	FROM    sys.database_permissions dp
	WHERE    USER_NAME(dp.grantee_principal_id) IN (@roleName)
	GROUP BY dp.state, dp.major_id, dp.permission_name, dp.class

	update #ScriptedRoles 
	set script = @roleDesc
	where name = @RoleName

end

select 
    name
,   script
from #ScriptedRoles
";
            Role r = null;
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    r = new Role {
                        Name = (string) dr["name"],
                        Script = (string) dr["script"]
                    };
                    roles.Add(r);
                }
            }
            return roles;
        }
    }
}