using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class RoutineComponent : DatabaseComponentBase<Routine> {
        private readonly Database _database;

        public Routine FindRoutine(List<Routine> routines, string name, string schema) {
            return routines.FirstOrDefault(r => r.Name == name && r.Owner == schema);
        }

        public RoutineComponent(Database database) {
            _database = database;
        }

        public override string Name {
            get { return "routines"; }
        }

        public override List<Routine> Load(SqlCommand cmd) {
            var routines = LoadRoutines(cmd);
            LoadXmlSchemas(cmd, routines);
            return routines;
        }

        private void LoadXmlSchemas(SqlCommand cm, List<Routine> routines) {
            try {
                // get xml schemas
                cm.CommandText = @"
						select s.name as DBSchemaName, x.name as XMLSchemaCollectionName, xml_schema_namespace(s.name, x.name) as definition
						from sys.xml_schema_collections x
						inner join sys.schemas s on s.schema_id = x.schema_idViewIndexes
						where s.name != 'sys'";
                using (var dr = cm.ExecuteReader()) {
                    while (dr.Read()) {
                        var r = new Routine((string) dr["DBSchemaName"], (string) dr["XMLSchemaCollectionName"],
                            _database) {
                                Text =
                                    string.Format("CREATE XML SCHEMA COLLECTION {0}.{1} AS N'{2}'", dr["DBSchemaName"],
                                        dr["XMLSchemaCollectionName"], dr["definition"]),
                                RoutineType = Routine.RoutineKind.XmlSchemaCollection
                            };
                        routines.Add(r);
                    }
                }
            } catch (SqlException) {
                // SQL server version doesn't support XML schemas, nothing to do here
            }
        }

        private List<Routine> LoadRoutines(SqlCommand cm) {
            var routines = new List<Routine>();

            //get routines
            cm.CommandText = @"
				select
					s.name as schemaName,
					o.name as routineName,
					o.type_desc,
					m.definition,
					m.uses_ansi_nulls,
					m.uses_quoted_identifier,
					isnull(s2.name, s3.name) as tableSchema,
					isnull(t.name, v.name) as tableName,
					tr.is_disabled as trigger_disabled
				from sys.sql_modules m
					inner join sys.objects o on m.object_id = o.object_id
					inner join sys.schemas s on s.schema_id = o.schema_id
					left join sys.triggers tr on m.object_id = tr.object_id
					left join sys.tables t on tr.parent_id = t.object_id
					left join sys.views v on tr.parent_id = v.object_id
					left join sys.schemas s2 on s2.schema_id = t.schema_id
					left join sys.schemas s3 on s3.schema_id = v.schema_id
				where objectproperty(o.object_id, 'IsMSShipped') = 0
				";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    var r = new Routine((string) dr["schemaName"], (string) dr["routineName"], _database);
                    r.Text = dr["definition"] is DBNull ? string.Empty : (string) dr["definition"];
                    r.AnsiNull = (bool) dr["uses_ansi_nulls"];
                    r.QuotedId = (bool) dr["uses_quoted_identifier"];
                    routines.Add(r);

                    switch ((string) dr["type_desc"]) {
                        case "SQL_STORED_PROCEDURE":
                            r.RoutineType = Routine.RoutineKind.Procedure;
                            break;
                        case "SQL_TRIGGER":
                            r.RoutineType = Routine.RoutineKind.Trigger;
                            r.RelatedTableName = (string) dr["tableName"];
                            r.RelatedTableSchema = (string) dr["tableSchema"];
                            r.Disabled = (bool) dr["trigger_disabled"];
                            break;
                        case "SQL_SCALAR_FUNCTION":
                        case "SQL_INLINE_TABLE_VALUED_FUNCTION":
                            r.RoutineType = Routine.RoutineKind.Function;
                            break;
                        case "VIEW":
                            r.RoutineType = Routine.RoutineKind.View;
                            break;
                    }
                }
            }
            return routines;
        }
    }
}