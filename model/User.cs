using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class User : DatabaseComponentBase<SqlUser> {
        public override string Name {
            get { return "users"; }
        }

        public override List<SqlUser> Load(SqlCommand cm) {
            var users = new List<SqlUser>();
            // get users that have access to the database
            cm.CommandText = @"
				select dp.name as UserName, USER_NAME(drm.role_principal_id) as AssociatedDBRole, default_schema_name
				from sys.database_principals dp
				left outer join sys.database_role_members drm on dp.principal_id = drm.member_principal_id
				where dp.type_desc = 'SQL_USER'
				and dp.sid not in (0x00, 0x01) --ignore guest and dbo
				and dp.is_fixed_role = 0
				order by dp.name";
            SqlUser u = null;
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    if (u == null || u.Name != (string) dr["UserName"])
                        u = new SqlUser((string) dr["UserName"], (string) dr["default_schema_name"]);
                    if (!(dr["AssociatedDBRole"] is DBNull))
                        u.DatabaseRoles.Add((string) dr["AssociatedDBRole"]);
                    if (!users.Contains(u))
                        users.Add(u);
                }
            }

            try {
                // get sql logins
                cm.CommandText = @"
					select sp.name,  sl.password_hash
					from sys.server_principals sp
					inner join sys.sql_logins sl on sp.principal_id = sl.principal_id and sp.type_desc = 'SQL_LOGIN'
					where sp.name not like '##%##'
					and sp.name != 'SA'
					order by sp.name";
                using (var dr = cm.ExecuteReader()) {
                    while (dr.Read()) {
                        u = FindUser(users, (string) dr["name"]);
                        if (u != null && !(dr["password_hash"] is DBNull))
                            u.PasswordHash = (byte[]) dr["password_hash"];
                    }
                }
            } catch (SqlException) {
                // SQL server version (i.e. Azure) doesn't support logins, nothing to do here
            }

            return users;
        }

        public SqlUser FindUser(List<SqlUser> users, string name) {
            return users.FirstOrDefault(u => string.Equals(u.Name, name, StringComparison.CurrentCultureIgnoreCase));
        }
    }
}