using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace SchemaZen.model {
    public class TableComponent : DatabaseComponentBase<Table> {


        public override string Name {
            get { return "tables"; }
        }

        public override List<Table> Load(SqlCommand cm) {
            var tables = LoadTables(cm);
            LoadColumns(cm, tables);
            LoadCheckConstraints(cm, tables);
            LoadColumnComputes(cm, tables);
            LoadColumnDefaults(cm, tables);
            LoadColumnIdentities(cm, tables);
            return tables;
        }



        private void LoadColumns(SqlCommand cm, IList<Table> tables)
        {
            //get columns
            cm.CommandText = @"
				select 
					t.TABLE_SCHEMA,
					c.TABLE_NAME,
					c.COLUMN_NAME,
					c.DATA_TYPE,
					c.ORDINAL_POSITION,
					c.IS_NULLABLE,
					c.CHARACTER_MAXIMUM_LENGTH,
					c.NUMERIC_PRECISION,
					c.NUMERIC_SCALE,
					CASE WHEN COLUMNPROPERTY(OBJECT_ID(c.TABLE_SCHEMA + '.' + c.TABLE_NAME), c.COLUMN_NAME, 'IsRowGuidCol') = 1 THEN 'YES' ELSE 'NO' END AS IS_ROW_GUID_COL
				from INFORMATION_SCHEMA.COLUMNS c
					inner join INFORMATION_SCHEMA.TABLES t
							on t.TABLE_NAME = c.TABLE_NAME
								and t.TABLE_SCHEMA = c.TABLE_SCHEMA
								and t.TABLE_CATALOG = c.TABLE_CATALOG
				where
					t.TABLE_TYPE = 'BASE TABLE'
				order by t.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
";
            using (var dr = cm.ExecuteReader())
            {
                LoadColumnsBase(dr, tables);
            }
        }


        private void LoadColumnIdentities(SqlCommand cm, IList<Table> tables)
        {
            //get column identities
            cm.CommandText = @"
					select 
						s.name as TABLE_SCHEMA,
						t.name as TABLE_NAME, 
						c.name AS COLUMN_NAME,
						i.SEED_VALUE, i.INCREMENT_VALUE
					from sys.tables t 
						inner join sys.columns c on c.object_id = t.object_id
						inner join sys.identity_columns i on i.object_id = c.object_id
							and i.column_id = c.column_id
						inner join sys.schemas s on s.schema_id = t.schema_id ";
            using (var dr = cm.ExecuteReader())
            {
                while (dr.Read())
                {
                    try
                    {
                        var t = FindTable(tables, (string)dr["TABLE_NAME"], (string)dr["TABLE_SCHEMA"]);
                        var c = t.Columns.Find((string)dr["COLUMN_NAME"]);
                        var seed = dr["SEED_VALUE"].ToString();
                        var increment = dr["INCREMENT_VALUE"].ToString();
                        c.Identity = new Identity(seed, increment);
                    }
                    catch (Exception ex)
                    {
                        throw new ApplicationException(
                            string.Format("{0}.{1} : {2}", dr["TABLE_SCHEMA"], dr["TABLE_NAME"], ex.Message), ex);
                    }
                }
            }
        }


        private void LoadColumnDefaults(SqlCommand cm, IList<Table> tables)
        {
            //get column defaults
            cm.CommandText = @"
					select 
						s.name as TABLE_SCHEMA,
						t.name as TABLE_NAME, 
						c.name as COLUMN_NAME, 
						d.name as DEFAULT_NAME, 
						d.definition as DEFAULT_VALUE
					from sys.tables t 
						inner join sys.columns c on c.object_id = t.object_id
						inner join sys.default_constraints d on c.column_id = d.parent_column_id
							and d.parent_object_id = c.object_id
						inner join sys.schemas s on s.schema_id = t.schema_id";
            using (var dr = cm.ExecuteReader())
            {
                while (dr.Read())
                {
                    var t = FindTable(tables, (string)dr["TABLE_NAME"], (string)dr["TABLE_SCHEMA"]);
                    t.Columns.Find((string)dr["COLUMN_NAME"]).Default = new Default((string)dr["DEFAULT_NAME"],
                        (string)dr["DEFAULT_VALUE"]);
                }
            }
        }

        private void LoadColumnComputes(SqlCommand cm, IList<Table> tables)
        {
            //get computed column definitions
            cm.CommandText = @"
					select
						object_schema_name(object_id) as TABLE_SCHEMA,
						object_name(object_id) as TABLE_NAME,
						name as COLUMN_NAME,
						definition as DEFINITION
					from sys.computed_columns cc
					";
            using (var dr = cm.ExecuteReader())
            {
                while (dr.Read())
                {
                    var t = FindTable(tables, (string)dr["TABLE_NAME"], (string)dr["TABLE_SCHEMA"]);
                    t.Columns.Find((string)dr["COLUMN_NAME"]).ComputedDefinition = (string)dr["DEFINITION"];
                }
            }
        }

        private List<Table> LoadTables(SqlCommand cm) {
            var tables = new List<Table>();
            //get tables
            cm.CommandText = @"
				select 
					TABLE_SCHEMA, 
					TABLE_NAME 
				from INFORMATION_SCHEMA.TABLES
				where TABLE_TYPE = 'BASE TABLE'";
            using (var dr = cm.ExecuteReader()) {
                LoadTablesBase(dr, false, tables);
            }
            return tables;
        }

        private void LoadCheckConstraints(SqlCommand cm, List<Table> tables)
        {

            cm.CommandText = @"

				WITH SysObjectCheckConstraints AS
				(
					SELECT OBJECT_NAME(OBJECT_ID) AS ConstraintName
						,SCHEMA_NAME(schema_id) AS SchemaName
						,OBJECT_NAME(parent_object_id) AS TableName
						,objectproperty(object_id, 'CnstIsNotRepl') AS NotForReplication
					FROM sys.objects
					WHERE type_desc = 'CHECK_CONSTRAINT'
				)

				SELECT CONSTRAINT_CATALOG AS TABLE_CATALOG, CONSTRAINT_SCHEMA AS TABLE_SCHEMA, 
						NotForReplication,
						TableName AS TABLE_NAME, CONSTRAINT_NAME, CHECK_CLAUSE 
				FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
				INNER JOIN SysObjectCheckConstraints ON 
				SysObjectCheckConstraints.SchemaName = CHECK_CONSTRAINTS.CONSTRAINT_SCHEMA AND
				SysObjectCheckConstraints.ConstraintName = CHECK_CONSTRAINTS.CONSTRAINT_NAME 

 
			";

            using (var dr = cm.ExecuteReader())
            {
                while (dr.Read())
                {
                    var t = FindTable(tables, (string)dr["TABLE_NAME"], (string)dr["TABLE_SCHEMA"]);
                    var constraint = Constraint.CreateCheckedConstraint(
                        (string)dr["CONSTRAINT_NAME"],
                        Convert.ToBoolean(dr["NotForReplication"]),
                        (string)dr["CHECK_CLAUSE"]
                        );

                    t.AddConstraint(constraint);
                }
            }
        }


    }
}