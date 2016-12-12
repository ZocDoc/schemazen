using System;
using System.Diagnostics;
using System.IO;

namespace SchemaZen.Library.Command {
    public class CreateCommand : BaseCommand {

        public void CreateDatabases(string databaseFilesPath)
        {
            var db = CreateDatabase();
            if (!Directory.Exists(db.Dir))
            {
                throw new FileNotFoundException(string.Format("Snapshot dir {0} does not exist.", db.Dir));
            }

            if (!Overwrite && (DBHelper.DbExists(db.Connection)))
            {
                var msg = string.Format("{0} {1} already exists - use overwrite property if you want to drop it",
    Server, DbName);
                throw new InvalidOperationException(msg);
            }

            db.CreateDBFromDir(databaseFilesPath, Logger.Log);
            Logger.Log(TraceLevel.Info, Environment.NewLine + "Database created successfully.");
        }

        public void CreateTables(string databaseFilesPath)
        {
            var db = CreateDatabase();
            if (!Directory.Exists(db.Dir))
            {
                throw new FileNotFoundException(string.Format("Snapshot dir {0} does not exist.", db.Dir));
            }

            db.CreateDbObjectsFromDir(databaseFilesPath, Logger.Log);
            Logger.Log(TraceLevel.Info, Environment.NewLine + "Database created successfully.");
        }
    }
}