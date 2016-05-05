using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class ForeignKeyComponent : DatabaseComponentBase<ForeignKey> {
        private readonly IList<Table> _tables;

        public ForeignKeyComponent(IList<Table> tables) {
            _tables = tables;
        }

        public override string Name {
            get { return "foreign_keys"; }
        }

        public override List<ForeignKey> Load(SqlCommand cm) {
            var foreignKeys = new List<ForeignKey>();
            //get foreign keys
            cm.CommandText = @"
					select 
						TABLE_SCHEMA,
						TABLE_NAME, 
						CONSTRAINT_NAME
					from INFORMATION_SCHEMA.TABLE_CONSTRAINTS
					where CONSTRAINT_TYPE = 'FOREIGN KEY'";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    var t = FindTable(_tables, (string) dr["TABLE_NAME"], (string) dr["TABLE_SCHEMA"]);
                    var fk = new ForeignKey((string) dr["CONSTRAINT_NAME"]);
                    fk.Table = t;
                    foreignKeys.Add(fk);
                }
            }

            //get foreign key props
            cm.CommandText = @"
					select 
						CONSTRAINT_NAME, 
						OBJECT_SCHEMA_NAME(fk.parent_object_id) as TABLE_SCHEMA,
						UPDATE_RULE, 
						DELETE_RULE,
						fk.is_disabled
					from INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
						inner join sys.foreign_keys fk on rc.CONSTRAINT_NAME = fk.name";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    var fk = FindForeignKey(foreignKeys, (string) dr["CONSTRAINT_NAME"], (string) dr["TABLE_SCHEMA"]);
                    fk.OnUpdate = (string) dr["UPDATE_RULE"];
                    fk.OnDelete = (string) dr["DELETE_RULE"];
                    fk.Check = !(bool) dr["is_disabled"];
                }
            }

            //get foreign key columns and ref table
            cm.CommandText = @"
select
	fk.name as CONSTRAINT_NAME,
	OBJECT_SCHEMA_NAME(fk.parent_object_id) as TABLE_SCHEMA,
	c1.name as COLUMN_NAME,
	OBJECT_SCHEMA_NAME(fk.referenced_object_id) as REF_TABLE_SCHEMA,
	OBJECT_NAME(fk.referenced_object_id) as REF_TABLE_NAME,
	c2.name as REF_COLUMN_NAME
from sys.foreign_keys fk
inner join sys.foreign_key_columns fkc
	on fkc.constraint_object_id = fk.object_id
inner join sys.columns c1
	on fkc.parent_column_id = c1.column_id
	and fkc.parent_object_id = c1.object_id
inner join sys.columns c2
	on fkc.referenced_column_id = c2.column_id
	and fkc.referenced_object_id = c2.object_id
order by fk.name, fkc.constraint_column_id
";
            using (var dr = cm.ExecuteReader()) {
                while (dr.Read()) {
                    var fk = FindForeignKey(foreignKeys, (string) dr["CONSTRAINT_NAME"], (string) dr["TABLE_SCHEMA"]);
                    if (fk == null) {
                        continue;
                    }
                    fk.Columns.Add((string) dr["COLUMN_NAME"]);
                    fk.RefColumns.Add((string) dr["REF_COLUMN_NAME"]);
                    if (fk.RefTable == null) {
                        fk.RefTable = FindTable(_tables, (string) dr["REF_TABLE_NAME"], (string) dr["REF_TABLE_SCHEMA"]);
                    }
                }
            }
            return foreignKeys;
        }

        public ForeignKey FindForeignKey(List<ForeignKey> foreignKeys, string name, string owner) {
            return foreignKeys.FirstOrDefault(fk => fk.Name == name && fk.Table.Owner == owner);
        }
    }
}