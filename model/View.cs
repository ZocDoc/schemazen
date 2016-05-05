using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class View : DatabaseComponentBase<Constraint> {
        private readonly IList<Table> _tables;

        public View(IList<Table> tables) {
            _tables = tables;
        }

        public override string Name {
            get { return "views"; }
        }

        public override List<Constraint> Load(SqlCommand cm) {
            var viewIndexes = new List<Constraint>();
            //get constraints & indexes
            cm.CommandText = @"
					select 
						s.name as schemaName,
						t.name as tableName, 
						t.baseType,
						i.name as indexName, 
						c.name as columnName,
						i.is_primary_key, 
						i.is_unique_constraint,
						i.is_unique, 
						i.type_desc,
						i.filter_definition,
						isnull(ic.is_included_column, 0) as is_included_column,
                        ic.is_descending_key
					from (
						select object_id, name, schema_id, 'T' as baseType
						from   sys.tables
						union
						select object_id, name, schema_id, 'V' as baseType
						from   sys.views
						union
						select type_table_object_id, name, schema_id, 'TVT' as baseType
						from   sys.table_types
						) t
						inner join sys.indexes i on i.object_id = t.object_id
						inner join sys.index_columns ic on ic.object_id = t.object_id
							and ic.index_id = i.index_id
						inner join sys.columns c on c.object_id = t.object_id
							and c.column_id = ic.column_id
						inner join sys.schemas s on s.schema_id = t.schema_id
					where i.type_desc != 'HEAP'
					order by s.name, t.name, i.name, ic.key_ordinal, ic.index_column_id";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    var schemaName = (string) dr["schemaName"];
                    var tableName = (string) dr["tableName"];
                    var indexName = (string) dr["indexName"];
                    var isView = (string) dr["baseType"] == "V";

                    var t = isView
                        ? new Table(schemaName, tableName)
                        : FindTable(_tables, tableName, schemaName);
                    var c = t.FindConstraint(indexName);

                    if (c == null) {
                        c = new Constraint(indexName, "", "");
                        t.AddConstraint(c);
                    }

                    if (isView) {
                        if (viewIndexes.Any(v => v.Name == indexName)) {
                            c = viewIndexes.First(v => v.Name == indexName);
                        } else {
                            viewIndexes.Add(c);
                        }
                    }
                    c.Clustered = (string) dr["type_desc"] == "CLUSTERED";
                    c.Unique = (bool) dr["is_unique"];
                    var filter = dr["filter_definition"].ToString(); //can be null
                    c.Filter = filter;
                    if ((bool) dr["is_included_column"]) {
                        c.IncludedColumns.Add((string) dr["columnName"]);
                    } else {
                        c.Columns.Add(new ConstraintColumn((string) dr["columnName"], (bool) dr["is_descending_key"]));
                    }

                    c.Type = "INDEX";
                    if ((bool) dr["is_primary_key"])
                        c.Type = "PRIMARY KEY";
                    if ((bool) dr["is_unique_constraint"])
                        c.Type = "UNIQUE";
                }
            }
            return viewIndexes;
        }

        public Constraint FindViewIndex(IList<Constraint> viewIndexes, string name) {
            return viewIndexes.FirstOrDefault(c => c.Name == name);
        }
    }
}