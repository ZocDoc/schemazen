using System;
using System.Collections.Generic;
using System.Data.SqlClient;

namespace SchemaZen.model {
    public class TableTypeComponent : DatabaseComponentBase<Table> {
        public override string Name {
            get { return "table_types"; }
        }

        public override List<Table> Load(SqlCommand cmd) {
            var tables = LoadTables(cmd);
            LoadColumns(cmd, tables);
            return tables;
        }


        private List<Table> LoadTables(SqlCommand cm) {
            var tablesTypes = new List<Table>();

            //get table types
            try {
                cm.CommandText = @"
				select 
					s.name as TABLE_SCHEMA,
					tt.name as TABLE_NAME
				from sys.table_types tt
				inner join sys.schemas s on tt.schema_id = s.schema_id
				where tt.is_user_defined = 1
				order by s.name, tt.name";
                using (var dr = cm.ExecuteReader()) {
                    LoadTablesBase(dr, true, tablesTypes);
                }
            } catch (SqlException) {
                // SQL server version doesn't support table types, nothing to do here
            }
            return tablesTypes;
        }

        private void LoadColumns(SqlCommand cm, IList<Table> TableTypes) {
            try {
                cm.CommandText = @"
				select 
					s.name as TABLE_SCHEMA,
					tt.name as TABLE_NAME, 
					c.name as COLUMN_NAME,
					t.name as DATA_TYPE,
					c.column_id as ORDINAL_POSITION,
					CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END as IS_NULLABLE,
					CASE WHEN t.name = 'nvarchar' and c.max_length > 0 THEN CAST(c.max_length as int)/2 ELSE CAST(c.max_length as int) END as CHARACTER_MAXIMUM_LENGTH,
					c.precision as NUMERIC_PRECISION,
					CAST(c.scale as int) as NUMERIC_SCALE,
					CASE WHEN c.is_rowguidcol = 1 THEN 'YES' ELSE 'NO' END as IS_ROW_GUID_COL
				from sys.columns c
					inner join sys.table_types tt
						on tt.type_table_object_id = c.object_id
					inner join sys.schemas s
						on tt.schema_id = s.schema_id 
					inner join sys.types t
						on t.system_type_id = c.system_type_id
							and t.user_type_id = c.user_type_id
				where
					tt.is_user_defined = 1
				order by s.name, tt.name, c.column_id
";
                using (var dr = cm.ExecuteReader()) {
                    LoadColumnsBase(dr, TableTypes);
                }
            } catch (SqlException) {
                // SQL server version doesn't support table types, nothing to do
            }
        }
    }
}