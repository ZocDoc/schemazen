using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class SynonymComponent : DatabaseComponentBase<Synonym> {
        public override string Name {
            get { return "synonyms"; }
        }

        public override List<Synonym> Load(SqlCommand cm) {
            var synonyms = new List<Synonym>();
            try {
                // get synonyms
                cm.CommandText = @"
						select object_schema_name(object_id) as schema_name, name as synonym_name, base_object_name
						from sys.synonyms";
                using (var dr = cm.ExecuteReader()) {
                    while (dr.Read()) {
                        var synonym = new Synonym((string) dr["synonym_name"], (string) dr["schema_name"]);
                        synonym.BaseObjectName = (string) dr["base_object_name"];
                        synonyms.Add(synonym);
                    }
                }
            } catch (SqlException) {
                // SQL server version doesn't support synonyms, nothing to do here
            }
            return synonyms;
        }

        public Synonym FindSynonym(List<Synonym> synonyms, string name, string schema) {
            return synonyms.FirstOrDefault(s => s.Name == name && s.Owner == schema);
        }
    }
}