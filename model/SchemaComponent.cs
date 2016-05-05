using System.Collections.Generic;
using System.Data.SqlClient;

namespace SchemaZen.model {
    public class SchemaComponent : DatabaseComponentBase<Schema> {
        public override string Name {
            get { return "schemas"; }
        }

        public override List<Schema> Load(SqlCommand cm) {
            var schemas = new List<Schema>();

            //get schemas
            cm.CommandText = @"
					select s.name as schemaName, p.name as principalName
					from sys.schemas s
					inner join sys.database_principals p on s.principal_id = p.principal_id
					where s.schema_id < 16384
					and s.name not in ('dbo','guest','sys','INFORMATION_SCHEMA')
					order by schema_id
";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    schemas.Add(new Schema((string) dr["schemaName"], (string) dr["principalName"]));
                }
            }
            return schemas;
        }
    }
}