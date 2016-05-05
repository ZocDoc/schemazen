using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class Assembly : DatabaseComponentBase<SqlAssembly> {
        public override string Name {
            get { return "assemblies"; }
        }


        public override List<SqlAssembly> Load(SqlCommand cm) {
            var assemblies = new List<SqlAssembly>();

            try {
                // get CLR assemblies
                cm.CommandText =
                    @"select a.name as AssemblyName, a.permission_set_desc, af.name as FileName, af.content
						from sys.assemblies a
						inner join sys.assembly_files af on a.assembly_id = af.assembly_id 
						where a.is_user_defined = 1
						order by a.name, af.file_id";
                SqlAssembly a = null;
                using (var dr = cm.ExecuteReader()) {
                    while (dr.Read()) {
                        if (a == null || a.Name != (string) dr["AssemblyName"])
                            a = new SqlAssembly((string) dr["permission_set_desc"], (string) dr["AssemblyName"]);
                        a.Files.Add(new KeyValuePair<string, byte[]>((string) dr["FileName"], (byte[]) dr["content"]));
                        if (!assemblies.Contains(a))
                            assemblies.Add(a);
                    }
                }
            } catch (SqlException) {
                // SQL server version doesn't support CLR assemblies, nothing to do here
            }
            return assemblies;
        }

        public SqlAssembly FindAssembly(List<SqlAssembly> assemblies, string name) {
            return assemblies.FirstOrDefault(a => a.Name == name);
        }
    }
}
