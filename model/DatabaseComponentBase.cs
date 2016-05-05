using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public abstract class DatabaseComponentBase<T> : IDatabaseComponent<T> {
//        protected List<Table> Tables;
//        protected List<Table> TableTypes;

//        public DatabaseComponentBase(List<Table> tables, List<Table> tableTypes) {
//            Tables = tables;
//            TableTypes = tableTypes;
//        }

        public abstract string Name { get; }
        public abstract List<T> Load(SqlCommand cmd);

        //        public Table FindTable(string name, string owner, bool isTableType = false)
        //        {
        //            return FindTableBase(isTableType ? TableTypes : Tables, name, owner);
        //        }
        //
        //        private static Table FindTableBase(IEnumerable<Table> tables, string name, string owner)
        //        {
        //            return tables.FirstOrDefault(t => t.Name == name && t.Owner == owner);
        //        }

        protected static void LoadTablesBase(SqlDataReader dr, bool areTableTypes, List<Table> tables)
        {
            while (dr.Read())
            {
                tables.Add(new Table((string)dr["TABLE_SCHEMA"], (string)dr["TABLE_NAME"]) { IsType = areTableTypes });
            }
        }


        protected static void LoadColumnsBase(IDataReader dr, IList<Table> tables)
        {
            Table table = null;

            while (dr.Read())
            {
                var c = new Column
                {
                    Name = (string)dr["COLUMN_NAME"],
                    Type = (string)dr["DATA_TYPE"],
                    IsNullable = (string)dr["IS_NULLABLE"] == "YES",
                    Position = (int)dr["ORDINAL_POSITION"],
                    IsRowGuidCol = (string)dr["IS_ROW_GUID_COL"] == "YES"
                };

                switch (c.Type)
                {
                    case "binary":
                    case "char":
                    case "nchar":
                    case "nvarchar":
                    case "varbinary":
                    case "varchar":
                        c.Length = (int)dr["CHARACTER_MAXIMUM_LENGTH"];
                        break;
                    case "decimal":
                    case "numeric":
                        c.Precision = (byte)dr["NUMERIC_PRECISION"];
                        c.Scale = (int)dr["NUMERIC_SCALE"];
                        break;
                }

                if (table == null || table.Name != (string)dr["TABLE_NAME"] || table.Owner != (string)dr["TABLE_SCHEMA"])
                    // only do a lookup if the table we have isn't already the relevant one
                    table = FindTable(tables, (string)dr["TABLE_NAME"], (string)dr["TABLE_SCHEMA"]);
                table.Columns.Add(c);
            }
        }

        protected static Table FindTable(IEnumerable<Table> tables, string name, string owner)
        {
            return tables.FirstOrDefault(t => t.Name == name && t.Owner == owner);
        }
    }
}