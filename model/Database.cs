using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;

namespace SchemaZen.model {
	public class Database {
		#region " Constructors "

		public Database() {
			Props.Add(new DbProp("COMPATIBILITY_LEVEL", ""));
			Props.Add(new DbProp("COLLATE", ""));
			Props.Add(new DbProp("AUTO_CLOSE", ""));
			Props.Add(new DbProp("AUTO_SHRINK", ""));
			Props.Add(new DbProp("ALLOW_SNAPSHOT_ISOLATION", ""));
			Props.Add(new DbProp("READ_COMMITTED_SNAPSHOT", ""));
			Props.Add(new DbProp("RECOVERY", ""));
			Props.Add(new DbProp("PAGE_VERIFY", ""));
			Props.Add(new DbProp("AUTO_CREATE_STATISTICS", ""));
			Props.Add(new DbProp("AUTO_UPDATE_STATISTICS", ""));
			Props.Add(new DbProp("AUTO_UPDATE_STATISTICS_ASYNC", ""));
			Props.Add(new DbProp("ANSI_NULL_DEFAULT", ""));
			Props.Add(new DbProp("ANSI_NULLS", ""));
			Props.Add(new DbProp("ANSI_PADDING", ""));
			Props.Add(new DbProp("ANSI_WARNINGS", ""));
			Props.Add(new DbProp("ARITHABORT", ""));
			Props.Add(new DbProp("CONCAT_NULL_YIELDS_NULL", ""));
			Props.Add(new DbProp("NUMERIC_ROUNDABORT", ""));
			Props.Add(new DbProp("QUOTED_IDENTIFIER", ""));
			Props.Add(new DbProp("RECURSIVE_TRIGGERS", ""));
			Props.Add(new DbProp("CURSOR_CLOSE_ON_COMMIT", ""));
			Props.Add(new DbProp("CURSOR_DEFAULT", ""));
			Props.Add(new DbProp("TRUSTWORTHY", ""));
			Props.Add(new DbProp("DB_CHAINING", ""));
			Props.Add(new DbProp("PARAMETERIZATION", ""));
			Props.Add(new DbProp("DATE_CORRELATION_OPTIMIZATION", ""));
		}

		public Database(string name)
			: this() {
			Name = name;
		}

		#endregion

		public const string SqlWhitespaceOrCommentRegex = @"(?>(?:\s+|--.*?(?:\r|\n)|/\*.*?\*/))";
		public const string SqlEnclosedIdentifierRegex = @"\[.+?\]";
		public const string SqlQuotedIdentifierRegex = "\".+?\"";

		public const string SqlRegularIdentifierRegex = @"(?!\d)[\w@$#]+";
			// see rules for regular identifiers here https://msdn.microsoft.com/en-us/library/ms175874.aspx

		#region " Properties "

		public string Connection = "";
		public List<Table> DataTables = new List<Table>();
		public string Dir = "";
		public List<ForeignKey> ForeignKeys = new List<ForeignKey>();
		public string Name;

		public List<DbProp> Props = new List<DbProp>();
		public List<Table> Tables = new List<Table>();

		public DbProp FindProp(string name) {
			return Props.FirstOrDefault(p => string.Equals(p.Name, name, StringComparison.CurrentCultureIgnoreCase));
		}

		public Constraint FindConstraint(string name) {
			return Tables.SelectMany(t => t.Constraints).FirstOrDefault(c => c.Name == name);
		}

		public List<Table> FindTablesRegEx(string pattern) {
			return Tables.Where(t => Regex.Match(t.Name, pattern).Success).ToList();
		}

        #endregion

        private static HashSet<string> _dirs = new HashSet<string> {
            "tables", "foreign_keys", "assemblies", "functions", "procedures", "triggers",
			"views", "xmlschemacollections", "data", "roles", "users", "synonyms", "table_types"
		};

		private void SetPropOnOff(string propName, object dbVal) {
			if (dbVal != DBNull.Value) {
				FindProp(propName).Value = (bool) dbVal ? "ON" : "OFF";
			}
		}

		private void SetPropString(string propName, object dbVal) {
			if (dbVal != DBNull.Value) {
				FindProp(propName).Value = dbVal.ToString();
			}
		}

		#region Load

		public void Load() {
			Tables.Clear();
//			TableTypes.Clear();
//			Routines.Clear();
            ForeignKeys.Clear();
			DataTables.Clear();
//			ViewIndexes.Clear();
//			Assemblies.Clear();
//			Users.Clear();
//			Synonyms.Clear();
//            Roles.Clear();

            //            "tables", "foreign_keys", "assemblies", "functions", "procedures", "triggers",
            //			"views", "xmlschemacollections", "data", "roles", "users", "synonyms", "table_types"

            using (var cn = new SqlConnection(Connection)) {
				cn.Open();
				using (var cm = cn.CreateCommand()) {
					LoadProps(cm);
//					LoadSchemas(cm);
//					LoadTables(cm);
//					LoadColumns(cm);
//					LoadColumnIdentities(cm);
//					LoadColumnDefaults(cm);
//					LoadColumnComputes(cm);
//					LoadConstraintsAndIndexes(cm);
//					LoadCheckConstraints(cm);
//					LoadForeignKeys(cm);
//					LoadRoutines(cm);
//					LoadXmlSchemas(cm);
//					LoadCLRAssemblies(cm);
//					LoadUsersAndLogins(cm);
//					LoadSynonyms(cm);
//                    LoadRoles(cm);
                }
			}
		}

		private void LoadProps(SqlCommand cm) {
			var cnStrBuilder = new SqlConnectionStringBuilder(Connection);
			// query schema for database properties
			cm.CommandText = @"
select
	[compatibility_level],
	[collation_name],
	[is_auto_close_on],
	[is_auto_shrink_on],
	[snapshot_isolation_state],
	[is_read_committed_snapshot_on],
	[recovery_model_desc],
	[page_verify_option_desc],
	[is_auto_create_stats_on],
	[is_auto_update_stats_on],
	[is_auto_update_stats_async_on],
	[is_ansi_null_default_on],
	[is_ansi_nulls_on],
	[is_ansi_padding_on],
	[is_ansi_warnings_on],
	[is_arithabort_on],
	[is_concat_null_yields_null_on],
	[is_numeric_roundabort_on],
	[is_quoted_identifier_on],
	[is_recursive_triggers_on],
	[is_cursor_close_on_commit_on],
	[is_local_cursor_default],
	[is_trustworthy_on],
	[is_db_chaining_on],
	[is_parameterization_forced],
	[is_date_correlation_on]
from sys.databases
where name = @dbname
";
			cm.Parameters.AddWithValue("@dbname", cnStrBuilder.InitialCatalog);
			using (IDataReader dr = cm.ExecuteReader()) {
				if (dr.Read()) {
					SetPropString("COMPATIBILITY_LEVEL", dr["compatibility_level"]);
					SetPropString("COLLATE", dr["collation_name"]);
					SetPropOnOff("AUTO_CLOSE", dr["is_auto_close_on"]);
					SetPropOnOff("AUTO_SHRINK", dr["is_auto_shrink_on"]);
					if (dr["snapshot_isolation_state"] != DBNull.Value) {
						FindProp("ALLOW_SNAPSHOT_ISOLATION").Value = (byte) dr["snapshot_isolation_state"] == 0 ||
																	 (byte) dr["snapshot_isolation_state"] == 2
							? "OFF"
							: "ON";
					}
					SetPropOnOff("READ_COMMITTED_SNAPSHOT", dr["is_read_committed_snapshot_on"]);
					SetPropString("RECOVERY", dr["recovery_model_desc"]);
					SetPropString("PAGE_VERIFY", dr["page_verify_option_desc"]);
					SetPropOnOff("AUTO_CREATE_STATISTICS", dr["is_auto_create_stats_on"]);
					SetPropOnOff("AUTO_UPDATE_STATISTICS", dr["is_auto_update_stats_on"]);
					SetPropOnOff("AUTO_UPDATE_STATISTICS_ASYNC", dr["is_auto_update_stats_async_on"]);
					SetPropOnOff("ANSI_NULL_DEFAULT", dr["is_ansi_null_default_on"]);
					SetPropOnOff("ANSI_NULLS", dr["is_ansi_nulls_on"]);
					SetPropOnOff("ANSI_PADDING", dr["is_ansi_padding_on"]);
					SetPropOnOff("ANSI_WARNINGS", dr["is_ansi_warnings_on"]);
					SetPropOnOff("ARITHABORT", dr["is_arithabort_on"]);
					SetPropOnOff("CONCAT_NULL_YIELDS_NULL", dr["is_concat_null_yields_null_on"]);
					SetPropOnOff("NUMERIC_ROUNDABORT", dr["is_numeric_roundabort_on"]);
					SetPropOnOff("QUOTED_IDENTIFIER", dr["is_quoted_identifier_on"]);
					SetPropOnOff("RECURSIVE_TRIGGERS", dr["is_recursive_triggers_on"]);
					SetPropOnOff("CURSOR_CLOSE_ON_COMMIT", dr["is_cursor_close_on_commit_on"]);
					if (dr["is_local_cursor_default"] != DBNull.Value) {
						FindProp("CURSOR_DEFAULT").Value = (bool) dr["is_local_cursor_default"] ? "LOCAL" : "GLOBAL";
					}
					SetPropOnOff("TRUSTWORTHY", dr["is_trustworthy_on"]);
					SetPropOnOff("DB_CHAINING", dr["is_db_chaining_on"]);
					if (dr["is_parameterization_forced"] != DBNull.Value) {
						FindProp("PARAMETERIZATION").Value = (bool) dr["is_parameterization_forced"] ? "FORCED" : "SIMPLE";
					}
					SetPropOnOff("DATE_CORRELATION_OPTIMIZATION", dr["is_date_correlation_on"]);
				}
			}
		}

		#endregion

		public DatabaseDiff Compare(Database db) {
			var diff = new DatabaseDiff();
			diff.Db = db;

			//compare database properties		   
			foreach (var p in from p in Props
				let p2 = db.FindProp(p.Name)
				where p.Script() != p2.Script()
				select p) {
				diff.PropsChanged.Add(p);
			}

			//get tables added and changed
			foreach (var tables in new[] {Tables, TableTypes}) {
				foreach (var t in tables) {
					var t2 = db.FindTable(t.Name, t.Owner, t.IsType);
					if (t2 == null) {
						diff.TablesAdded.Add(t);
					} else {
						//compare mutual tables
						var tDiff = t.Compare(t2);
						if (tDiff.IsDiff) {
							if (t.IsType) {
								// types cannot be altered...
								diff.TableTypesDiff.Add(t);
							} else {
								diff.TablesDiff.Add(tDiff);
							}
						}
					}
				}
			}
			//get deleted tables
			foreach (var t in db.Tables.Concat(db.TableTypes).Where(t => FindTable(t.Name, t.Owner, t.IsType) == null)) {
				diff.TablesDeleted.Add(t);
			}

			//get procs added and changed
			foreach (var r in Routines) {
				var r2 = db.FindRoutine(r.Name, r.Owner);
				if (r2 == null) {
					diff.RoutinesAdded.Add(r);
				} else {
					//compare mutual procs
					if (r.Text.Trim() != r2.Text.Trim()) {
						diff.RoutinesDiff.Add(r);
					}
				}
			}
			//get procs deleted
			foreach (var r in db.Routines.Where(r => FindRoutine(r.Name, r.Owner) == null)) {
				diff.RoutinesDeleted.Add(r);
			}

			//get added and compare mutual foreign keys
			foreach (var fk in ForeignKeys) {
				var fk2 = db.FindForeignKey(fk.Name, fk.Table.Owner);
				if (fk2 == null) {
					diff.ForeignKeysAdded.Add(fk);
				} else {
					if (fk.ScriptCreate() != fk2.ScriptCreate()) {
						diff.ForeignKeysDiff.Add(fk);
					}
				}
			}
			//get deleted foreign keys
			foreach (var fk in db.ForeignKeys.Where(fk => FindForeignKey(fk.Name, fk.Table.Owner) == null)) {
				diff.ForeignKeysDeleted.Add(fk);
			}


			//get added and compare mutual assemblies
			foreach (var a in Assemblies) {
				var a2 = db.FindAssembly(a.Name);
				if (a2 == null) {
					diff.AssembliesAdded.Add(a);
				} else {
					if (a.ScriptCreate() != a2.ScriptCreate()) {
						diff.AssembliesDiff.Add(a);
					}
				}
			}
			//get deleted assemblies
			foreach (var a in db.Assemblies.Where(a => FindAssembly(a.Name) == null)) {
				diff.AssembliesDeleted.Add(a);
			}


			//get added and compare mutual users
			foreach (var u in Users) {
				var u2 = db.FindUser(u.Name);
				if (u2 == null) {
					diff.UsersAdded.Add(u);
				} else {
					if (u.ScriptCreate() != u2.ScriptCreate()) {
						diff.UsersDiff.Add(u);
					}
				}
			}
			//get deleted users
			foreach (var u in db.Users.Where(u => FindUser(u.Name) == null)) {
				diff.UsersDeleted.Add(u);
			}

			//get added and compare view indexes
			foreach (var c in ViewIndexes) {
				var c2 = db.FindViewIndex(c.Name);
				if (c2 == null) {
					diff.ViewIndexesAdded.Add(c);
				} else {
					if (c.ScriptCreate() != c2.ScriptCreate()) {
						diff.ViewIndexesDiff.Add(c);
					}
				}
			}
			//get deleted view indexes
			foreach (var c in db.ViewIndexes.Where(c => FindViewIndex(c.Name) == null)) {
				diff.ViewIndexesDeleted.Add(c);
			}

			//get added and compare synonyms
			foreach (var s in Synonyms) {
				var s2 = db.FindSynonym(s.Name, s.Owner);
				if (s2 == null) {
					diff.SynonymsAdded.Add(s);
				} else {
					if (s.BaseObjectName != s2.BaseObjectName) {
						diff.SynonymsDiff.Add(s);
					}
				}
			}
			//get deleted synonyms
			foreach (var s in db.Synonyms.Where(s => FindSynonym(s.Name, s.Owner) == null)) {
				diff.SynonymsDeleted.Add(s);
			}

			return diff;
		}

		public string ScriptCreate() {
			var text = new StringBuilder();

			text.AppendFormat("CREATE DATABASE {0}", Name);
			text.AppendLine();
			text.AppendLine("GO");
			text.AppendFormat("USE {0}", Name);
			text.AppendLine();
			text.AppendLine("GO");
			text.AppendLine();

			if (Props.Count > 0) {
				text.Append(ScriptPropList(Props));
				text.AppendLine("GO");
				text.AppendLine();
			}

			foreach (var schema in Schemas) {
				text.AppendLine(schema.ScriptCreate());
				text.AppendLine("GO");
				text.AppendLine();
			}

			foreach (var t in Tables.Concat(TableTypes)) {
				text.AppendLine(t.ScriptCreate());
			}
			text.AppendLine();
			text.AppendLine("GO");

			foreach (var fk in ForeignKeys) {
				text.AppendLine(fk.ScriptCreate());
			}
			text.AppendLine();
			text.AppendLine("GO");

			foreach (var r in Routines) {
				text.AppendLine(r.ScriptCreate());
				text.AppendLine();
				text.AppendLine("GO");
			}

			foreach (var a in Assemblies) {
				text.AppendLine(a.ScriptCreate());
				text.AppendLine();
				text.AppendLine("GO");
			}

			foreach (var u in Users) {
				text.AppendLine(u.ScriptCreate());
				text.AppendLine();
				text.AppendLine("GO");
			}

			foreach (var c in ViewIndexes) {
				text.AppendLine(c.ScriptCreate());
				text.AppendLine();
				text.AppendLine("GO");
			}

			foreach (var s in Synonyms) {
				text.AppendLine(s.ScriptCreate());
				text.AppendLine();
				text.AppendLine("GO");
			}

			return text.ToString();
		}

		#region Script

		public void ScriptToDir(string tableHint = null, Action<TraceLevel, string> log = null) {
			if (log == null) log = (tl, s) => { };

			if (Directory.Exists(Dir)) {
				// delete the existing script files
				log(TraceLevel.Verbose, "Deleting existing files...");

				var files = dirs.Select(dir => Path.Combine(Dir, dir))
					.Where(Directory.Exists).SelectMany(Directory.GetFiles);
				foreach (var f in files) {
					File.Delete(f);
				}
				log(TraceLevel.Verbose, "Existing files deleted.");
			} else {
				Directory.CreateDirectory(Dir);
			}

			WritePropsScript(log);
			WriteSchemaScript(log);
			WriteScriptDir("tables", Tables.ToArray(), log);
			WriteScriptDir("table_types", TableTypes.ToArray(), log);
			WriteScriptDir("foreign_keys", ForeignKeys.ToArray(), log);
			foreach (var routineType in Routines.GroupBy(x => x.RoutineType)) {
				var dir = routineType.Key.ToString().ToLower() + "s";
				WriteScriptDir(dir, routineType.ToArray(), log);
			}
			WriteScriptDir("views", ViewIndexes.ToArray(), log);
			WriteScriptDir("assemblies", Assemblies.ToArray(), log);
            WriteScriptDir("roles", Roles.ToArray(), log);
            WriteScriptDir("users", Users.ToArray(), log);
			WriteScriptDir("synonyms", Synonyms.ToArray(), log);

			ExportData(tableHint, log);
		}

		private void WritePropsScript(Action<TraceLevel, string> log) {
			log(TraceLevel.Verbose, "Scripting database properties...");
			var text = new StringBuilder();
			text.Append(ScriptPropList(Props));
			text.AppendLine("GO");
			text.AppendLine();
			File.WriteAllText(string.Format("{0}/props.sql", Dir), text.ToString());
		}

		private void WriteSchemaScript(Action<TraceLevel, string> log) {
			log(TraceLevel.Verbose, "Scripting database schemas...");
			var text = new StringBuilder();
			foreach (var schema in Schemas) {
				text.Append(schema.ScriptCreate());
			}
			text.AppendLine("GO");
			text.AppendLine();
			File.WriteAllText(string.Format("{0}/schemas.sql", Dir), text.ToString());
		}

		private void WriteScriptDir(string name, ICollection<IScriptable> objects, Action<TraceLevel, string> log) {
			if (!objects.Any()) return;
			var dir = Path.Combine(Dir, name);
			Directory.CreateDirectory(dir);
			var index = 0;
			foreach (var o in objects) {
				log(TraceLevel.Verbose, string.Format("Scripting {0} {1} of {2}...{3}", name, ++index, objects.Count, index < objects.Count ? "\r" : string.Empty));
				var filePath = Path.Combine(dir, MakeFileName(o) + ".sql");
				var script = o.ScriptCreate() + "\r\nGO\r\n";
				File.AppendAllText(filePath, script);
			}
		}

		private static string MakeFileName(object o) {
			// combine foreign keys into one script per table
			var fk = o as ForeignKey;
			if (fk != null) return MakeFileName(fk.Table);

			var schema = (o as IHasOwner) == null ? "" : (o as IHasOwner).Owner;
			var name = (o as INameable) == null ? "" : (o as INameable).Name;

			var fileName = MakeFileName(schema, name);

			// prefix user defined types with TYPE_
			var prefix = (o as Table) == null ? "" : (o as Table).IsType ? "TYPE_" : "";

			return string.Concat(prefix, fileName);
		}

		private static string MakeFileName(string schema, string name) {
			// Dont' include schema name for objects in the dbo schema.
			// This maintains backward compatability for those who use
			// SchemaZen to keep their schemas under version control.
			var fileName = name;
			if (!string.IsNullOrEmpty(schema) && schema.ToLower() != "dbo") {
				fileName = string.Format("{0}.{1}", schema, name);
			}
			foreach (var invalidChar in Path.GetInvalidFileNameChars())
				fileName = fileName.Replace(invalidChar, '-');
			return fileName;
		}

		public void ExportData(string tableHint = null, Action<TraceLevel, string> log = null) {
			if (!DataTables.Any())
				return;
			var dataDir = Dir + "/data";
			if (!Directory.Exists(dataDir)) {
				Directory.CreateDirectory(dataDir);
			}
			if (log != null)
				log(TraceLevel.Info, "Exporting data...");
			var index = 0;
			foreach (var t in DataTables) {
				if (log != null)
					log(TraceLevel.Verbose, string.Format("Exporting data from {0} (table {1} of {2})...", t.Owner + "." + t.Name, ++index, DataTables.Count));
			    var filePathAndName = dataDir + "/" + MakeFileName(t) + ".tsv";
			    var sw = File.CreateText(filePathAndName);
				t.ExportData(Connection, sw, tableHint);

                sw.Flush();
			    if (sw.BaseStream.Length == 0) {
                    if (log != null)
                        log(TraceLevel.Verbose, string.Format("          No data to export for {0}, deleting file...", t.Owner + "." + t.Name));
                    sw.Close();
                    File.Delete(filePathAndName);
                } else {
                    sw.Close();
			    }
			}
		}

		public static string ScriptPropList(IList<DbProp> props) {
			var text = new StringBuilder();

			text.AppendLine("DECLARE @DB VARCHAR(255)");
			text.AppendLine("SET @DB = DB_NAME()");
			foreach (var p in props.Select(p => p.Script()).Where(p => !string.IsNullOrEmpty(p))) {
				text.AppendLine(p);
			}
			return text.ToString();
		}

		#endregion

		#region Create

		public void ImportData(Action<TraceLevel, string> log = null) {
			if (log == null) log = (tl, s) => { };

			var dataDir = Dir + "\\data";
			if (!Directory.Exists(dataDir)) {
				log(TraceLevel.Verbose, "No data to import.");
				return;
			}

			log(TraceLevel.Verbose, "Loading database schema...");
			Load(); // load the schema first so we can import data
			log(TraceLevel.Verbose, "Database schema loaded.");
			log(TraceLevel.Info, "Importing data...");

			foreach (var f in Directory.GetFiles(dataDir)) {
				var fi = new FileInfo(f);
				var schema = "dbo";
				var table = Path.GetFileNameWithoutExtension(fi.Name);
				if (table.Contains(".")) {
					schema = fi.Name.Split('.')[0];
					table = fi.Name.Split('.')[1];
				}
				var t = FindTable(table, schema);
				if (t == null) {
					log(TraceLevel.Warning, string.Format("Warning: found data file '{0}', but no corresponding table in database...", fi.Name));
					continue;
				}
				try {
					log(TraceLevel.Verbose, string.Format("Importing data for table {0}.{1}...", schema, table));
					t.ImportData(Connection, fi.FullName);
				} catch (SqlBatchException ex) {
					throw new DataFileException(ex.Message, fi.FullName, ex.LineNumber);
				} catch (Exception ex) {
					throw new DataFileException(ex.Message, fi.FullName, -1);
				}
			}
			log(TraceLevel.Info, "Data imported successfully.");
		}

		public void CreateFromDir(bool overwrite, string databaseFilesPath = null, Action<TraceLevel, string> log = null) {
			if (log == null) log = (tl, s) => { };

			if (DBHelper.DbExists(Connection)) {
				log(TraceLevel.Verbose, "Dropping existing database...");
				DBHelper.DropDb(Connection);
				log(TraceLevel.Verbose, "Existing database dropped.");
			}

			log(TraceLevel.Info, "Creating database...");
			//create database
			DBHelper.CreateDb(Connection, databaseFilesPath);

			//run scripts
			if (File.Exists(Dir + "/props.sql")) {
				log(TraceLevel.Verbose, "Setting database properties...");
				try {
					DBHelper.ExecBatchSql(Connection, File.ReadAllText(Dir + "/props.sql"));
				} catch (SqlBatchException ex) {
					throw new SqlFileException(Dir + "/props.sql", ex);
				}

				// COLLATE can cause connection to be reset
				// so clear the pool so we get a new connection
				DBHelper.ClearPool(Connection);
			}

			if (File.Exists(Dir + "/schemas.sql")) {
				log(TraceLevel.Verbose, "Creating database schemas...");
				try {
					DBHelper.ExecBatchSql(Connection, File.ReadAllText(Dir + "/schemas.sql"));
				} catch (SqlBatchException ex) {
					throw new SqlFileException(Dir + "/schemas.sql", ex);
				}
			}

			log(TraceLevel.Info, "Creating database objects...");
			// create db objects

			// resolve dependencies by trying over and over
			// if the number of failures stops decreasing then give up
			var scripts = GetScripts();
			var errors = new List<SqlFileException>();
			var prevCount = -1;
			while (scripts.Count > 0 && (prevCount == -1 || errors.Count < prevCount)) {
				if (errors.Count > 0) {
					prevCount = errors.Count;
					log(TraceLevel.Info, string.Format(
						"{0} errors occurred, retrying...", errors.Count));
				}
				errors.Clear();
				var index = 0;
				var total = scripts.Count;
				foreach (var f in scripts.ToArray()) {
					log(TraceLevel.Verbose, string.Format("Executing script {0} of {1}...{2}", ++index, total, index < total ? "\r" : string.Empty));
					try {
						DBHelper.ExecBatchSql(Connection, File.ReadAllText(f));
						scripts.Remove(f);
					} catch (SqlBatchException ex) {
						errors.Add(new SqlFileException(f, ex));
						//Console.WriteLine("Error occurred in {0}: {1}", f, ex);
					}
				}
			}
			if (prevCount > 0)
				log(TraceLevel.Info, errors.Any() ? string.Format("{0} errors unresolved. Details will follow later.", prevCount) : "All errors resolved, were probably dependency issues...");
			log(TraceLevel.Info, string.Empty);

			ImportData(log); // load data

			if (Directory.Exists(Dir + "/after_data")) {
				log(TraceLevel.Verbose, "Executing after-data scripts...");
				foreach (var f in Directory.GetFiles(Dir + "/after_data", "*.sql")) {
					try {
						DBHelper.ExecBatchSql(Connection, File.ReadAllText(f));
					} catch (SqlBatchException ex) {
						errors.Add(new SqlFileException(f, ex));
					}
				}
			}

			// foreign keys
			if (Directory.Exists(Dir + "/foreign_keys")) {
				log(TraceLevel.Info, "Adding foreign key constraints...");
				foreach (var f in Directory.GetFiles(Dir + "/foreign_keys", "*.sql")) {
					try {
						DBHelper.ExecBatchSql(Connection, File.ReadAllText(f));
					} catch (SqlBatchException ex) {
						//throw new SqlFileException(f, ex);
						errors.Add(new SqlFileException(f, ex));
					}
				}
			}
			if (errors.Count > 0) {
				var ex = new BatchSqlFileException();
				ex.Exceptions = errors;
				throw ex;
			}
		}

		private List<string> GetScripts() {
			var scripts = new List<string>();
			foreach (
				var dirPath in dirs.Where(dir => dir != "foreign_keys").Select(dir => Dir + "/" + dir).Where(Directory.Exists)) {
				scripts.AddRange(Directory.GetFiles(dirPath, "*.sql"));
			}
			return scripts;
		}

		public void ExecCreate(bool dropIfExists) {
			var conStr = new SqlConnectionStringBuilder(Connection);
			var dbName = conStr.InitialCatalog;
			conStr.InitialCatalog = "master";
			if (DBHelper.DbExists(Connection)) {
				if (dropIfExists) {
					DBHelper.DropDb(Connection);
				} else {
					throw new ApplicationException(string.Format("Database {0} {1} already exists.",
						conStr.DataSource, dbName));
				}
			}
			DBHelper.ExecBatchSql(conStr.ToString(), ScriptCreate());
		}

		#endregion
	}

    //            "tables", "foreign_keys", "assemblies", "functions", "procedures", "triggers",
    //			"views", "xmlschemacollections", "data", "roles", "users", "synonyms", "table_types"
//            WriteScriptDir("tables", Tables.ToArray(), log);
//
//            WriteScriptDir("table_types", TableTypes.ToArray(), log);
//
//            WriteScriptDir("foreign_keys", ForeignKeys.ToArray(), log);
//			foreach (var routineType in Routines.GroupBy(x => x.RoutineType)) {
//				var dir = routineType.Key.ToString().ToLower() + "s";
//
//                WriteScriptDir(dir, routineType.ToArray(), log);
//			}
//
//            WriteScriptDir("views", ViewIndexes.ToArray(), log);
//
//            WriteScriptDir("assemblies", Assemblies.ToArray(), log);
//            WriteScriptDir("roles", Roles.ToArray(), log);
//            WriteScriptDir("users", Users.ToArray(), log);
//
//            WriteScriptDir("synonyms", Synonyms.ToArray(), log);


    public class DatabaseDiff {
		public List<SqlAssembly> AssembliesAdded = new List<SqlAssembly>();
		public List<SqlAssembly> AssembliesDeleted = new List<SqlAssembly>();
		public List<SqlAssembly> AssembliesDiff = new List<SqlAssembly>();
		public Database Db;
		public List<ForeignKey> ForeignKeysAdded = new List<ForeignKey>();
		public List<ForeignKey> ForeignKeysDeleted = new List<ForeignKey>();
		public List<ForeignKey> ForeignKeysDiff = new List<ForeignKey>();
		public List<DbProp> PropsChanged = new List<DbProp>();

		public List<Routine> RoutinesAdded = new List<Routine>();
		public List<Routine> RoutinesDeleted = new List<Routine>();
		public List<Routine> RoutinesDiff = new List<Routine>();
		public List<Synonym> SynonymsAdded = new List<Synonym>();
		public List<Synonym> SynonymsDeleted = new List<Synonym>();
		public List<Synonym> SynonymsDiff = new List<Synonym>();
		public List<Table> TablesAdded = new List<Table>();
		public List<Table> TablesDeleted = new List<Table>();
		public List<TableDiff> TablesDiff = new List<TableDiff>();
		public List<Table> TableTypesDiff = new List<Table>();
		public List<SqlUser> UsersAdded = new List<SqlUser>();
		public List<SqlUser> UsersDeleted = new List<SqlUser>();
		public List<SqlUser> UsersDiff = new List<SqlUser>();
		public List<Constraint> ViewIndexesAdded = new List<Constraint>();
		public List<Constraint> ViewIndexesDeleted = new List<Constraint>();
		public List<Constraint> ViewIndexesDiff = new List<Constraint>();

		public bool IsDiff {
			get {
				return PropsChanged.Count > 0
					   || TablesAdded.Count > 0
					   || TablesDiff.Count > 0
					   || TableTypesDiff.Count > 0
					   || TablesDeleted.Count > 0
					   || RoutinesAdded.Count > 0
					   || RoutinesDiff.Count > 0
					   || RoutinesDeleted.Count > 0
					   || ForeignKeysAdded.Count > 0
					   || ForeignKeysDiff.Count > 0
					   || ForeignKeysDeleted.Count > 0
					   || AssembliesAdded.Count > 0
					   || AssembliesDiff.Count > 0
					   || AssembliesDeleted.Count > 0
					   || UsersAdded.Count > 0
					   || UsersDiff.Count > 0
					   || UsersDeleted.Count > 0
					   || ViewIndexesAdded.Count > 0
					   || ViewIndexesDiff.Count > 0
					   || ViewIndexesDeleted.Count > 0
					   || SynonymsAdded.Count > 0
					   || SynonymsDiff.Count > 0
					   || SynonymsDeleted.Count > 0;
			}
		}

		private static string Summarize(bool includeNames, List<string> changes, string caption) {
			if (changes.Count == 0) return string.Empty;
			return changes.Count + "x " + caption +
				   (includeNames ? ("\r\n\t" + string.Join("\r\n\t", changes.ToArray())) : string.Empty) + "\r\n";
		}

		public string SummarizeChanges(bool includeNames) {
			var sb = new StringBuilder();
			sb.Append(Summarize(includeNames, AssembliesAdded.Select(o => o.Name).ToList(),
				"assemblies in source but not in target"));
			sb.Append(Summarize(includeNames, AssembliesDeleted.Select(o => o.Name).ToList(),
				"assemblies not in source but in target"));
			sb.Append(Summarize(includeNames, AssembliesDiff.Select(o => o.Name).ToList(), "assemblies altered"));
			sb.Append(Summarize(includeNames, ForeignKeysAdded.Select(o => o.Name).ToList(),
				"foreign keys in source but not in target"));
			sb.Append(Summarize(includeNames, ForeignKeysDeleted.Select(o => o.Name).ToList(),
				"foreign keys not in source but in target"));
			sb.Append(Summarize(includeNames, ForeignKeysDiff.Select(o => o.Name).ToList(), "foreign keys altered"));
			sb.Append(Summarize(includeNames, PropsChanged.Select(o => o.Name).ToList(), "properties changed"));
			sb.Append(Summarize(includeNames,
				RoutinesAdded.Select(o => string.Format("{0} {1}.{2}", o.RoutineType.ToString(), o.Owner, o.Name)).ToList(),
				"routines in source but not in target"));
			sb.Append(Summarize(includeNames,
				RoutinesDeleted.Select(o => string.Format("{0} {1}.{2}", o.RoutineType.ToString(), o.Owner, o.Name)).ToList(),
				"routines not in source but in target"));
			sb.Append(Summarize(includeNames,
				RoutinesDiff.Select(o => string.Format("{0} {1}.{2}", o.RoutineType.ToString(), o.Owner, o.Name)).ToList(),
				"routines altered"));
			sb.Append(Summarize(includeNames,
				TablesAdded.Where(o => !o.IsType).Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"tables in source but not in target"));
			sb.Append(Summarize(includeNames,
				TablesDeleted.Where(o => !o.IsType).Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"tables not in source but in target"));
			sb.Append(Summarize(includeNames, TablesDiff.Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"tables altered"));
			sb.Append(Summarize(includeNames,
				TablesAdded.Where(o => o.IsType).Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"table types in source but not in target"));
			sb.Append(Summarize(includeNames,
				TablesDeleted.Where(o => o.IsType).Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"table types not in source but in target"));
			sb.Append(Summarize(includeNames, TableTypesDiff.Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"table types altered"));
			sb.Append(Summarize(includeNames, UsersAdded.Select(o => o.Name).ToList(), "users in source but not in target"));
			sb.Append(Summarize(includeNames, UsersDeleted.Select(o => o.Name).ToList(), "users not in source but in target"));
			sb.Append(Summarize(includeNames, UsersDiff.Select(o => o.Name).ToList(), "users altered"));
			sb.Append(Summarize(includeNames, ViewIndexesAdded.Select(o => o.Name).ToList(),
				"view indexes in source but not in target"));
			sb.Append(Summarize(includeNames, ViewIndexesDeleted.Select(o => o.Name).ToList(),
				"view indexes not in source but in target"));
			sb.Append(Summarize(includeNames, ViewIndexesDiff.Select(o => o.Name).ToList(), "view indexes altered"));
			sb.Append(Summarize(includeNames, SynonymsAdded.Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"synonyms in source but not in target"));
			sb.Append(Summarize(includeNames, SynonymsDeleted.Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"synonyms not in source but in target"));
			sb.Append(Summarize(includeNames, SynonymsDiff.Select(o => string.Format("{0}.{1}", o.Owner, o.Name)).ToList(),
				"synonyms altered"));
			return sb.ToString();
		}

		public string Script() {
			var text = new StringBuilder();
			//alter database props
			//TODO need to check dependencies for collation change
			//TODO how can collation be set to null at the server level?
			if (PropsChanged.Count > 0) {
				text.Append(Database.ScriptPropList(PropsChanged));
				text.AppendLine("GO");
				text.AppendLine();
			}

			//delete foreign keys
			if (ForeignKeysDeleted.Count + ForeignKeysDiff.Count > 0) {
				foreach (var fk in ForeignKeysDeleted) {
					text.AppendLine(fk.ScriptDrop());
				}
				//delete modified foreign keys
				foreach (var fk in ForeignKeysDiff) {
					text.AppendLine(fk.ScriptDrop());
				}
				text.AppendLine("GO");
			}

			//delete tables
			if (TablesDeleted.Count + TableTypesDiff.Count > 0) {
				foreach (var t in TablesDeleted.Concat(TableTypesDiff)) {
					text.AppendLine(t.ScriptDrop());
				}
				text.AppendLine("GO");
			}
			// TODO: table types drop will fail if anything references them... try to find a workaround?


			//modify tables
			if (TablesDiff.Count > 0) {
				foreach (var t in TablesDiff) {
					text.Append(t.Script());
				}
				text.AppendLine("GO");
			}

			//add tables
			if (TablesAdded.Count + TableTypesDiff.Count > 0) {
				foreach (var t in TablesAdded.Concat(TableTypesDiff)) {
					text.Append(t.ScriptCreate());
				}
				text.AppendLine("GO");
			}

			//add foreign keys
			if (ForeignKeysAdded.Count + ForeignKeysDiff.Count > 0) {
				foreach (var fk in ForeignKeysAdded) {
					text.AppendLine(fk.ScriptCreate());
				}
				//add modified foreign keys
				foreach (var fk in ForeignKeysDiff) {
					text.AppendLine(fk.ScriptCreate());
				}
				text.AppendLine("GO");
			}

			//add & delete procs, functions, & triggers
			foreach (var r in RoutinesAdded) {
				text.AppendLine(r.ScriptCreate());
				text.AppendLine("GO");
			}
			foreach (var r in RoutinesDiff) {
				// script alter if possible, otherwise drop and (re)create
				try {
					text.AppendLine(r.ScriptAlter(Db));
					text.AppendLine("GO");
				} catch {
					text.AppendLine(r.ScriptDrop());
					text.AppendLine("GO");
					text.AppendLine(r.ScriptCreate());
					text.AppendLine("GO");
				}
			}
			foreach (var r in RoutinesDeleted) {
				text.AppendLine(r.ScriptDrop());
				text.AppendLine("GO");
			}

			//add & delete synonyms
			foreach (var s in SynonymsAdded) {
				text.AppendLine(s.ScriptCreate());
				text.AppendLine("GO");
			}
			foreach (var s in SynonymsDiff) {
				text.AppendLine(s.ScriptDrop());
				text.AppendLine("GO");
				text.AppendLine(s.ScriptCreate());
				text.AppendLine("GO");
			}
			foreach (var s in SynonymsDeleted) {
				text.AppendLine(s.ScriptDrop());
				text.AppendLine("GO");
			}

			return text.ToString();
		}
	}
}
