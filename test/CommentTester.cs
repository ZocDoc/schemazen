﻿using System;
using System.IO;
using NUnit.Framework;
using SchemaZen.Library;
using SchemaZen.Library.Models;

namespace SchemaZen.Tests
{
    [TestFixture]
    class CommentTester {
        private const string SetupTable0Script = @"
CREATE TABLE [dbo].[TestTable0] (
   [VariantVersionId] [smallint] NOT NULL   ,
   [MetricTypeId] [smallint] NOT NULL   ,
   [RequestId] [bigint] NOT NULL   ,
   [ProfessionalId] [bigint] NOT NULL   ,
   [ZdAppointmentId] [bigint] NOT NULL   
   ,CONSTRAINT [PK_AbAdvancedMetrics_VariantVersionId_MetricTypeId_RequestId] PRIMARY KEY CLUSTERED ([VariantVersionId], [MetricTypeId], [RequestId]),
     CONSTRAINT AK_Uni UNIQUE(MetricTypeId)
)";

        private const string SetupTable1Script = @"
CREATE TABLE [dbo].[TestTable1] (
   [MetricTypeId] [smallint] NOT NULL   
   CONSTRAINT AK_Metric UNIQUE(MetricTypeId)   
)

";

        private const string SetUpTable2Script = @"
CREATE TABLE [dbo].[TestTable2] (
ID int
)
";

        private const string SetupTableTypeScript = @"
CREATE TYPE [dbo].[TestTableType] AS TABLE(
	[ID] [nvarchar](250) NULL,
	[Value] [numeric](5, 1) NULL,
	[LongNVarchar] [nvarchar](max) NULL
)

";
        private const string SetupFKScript = @"
ALTER TABLE [dbo].[TestTable0]  
  ADD CONSTRAINT TestConstraint
  FOREIGN KEY([VariantVersionId]) REFERENCES [dbo].[TestTable1](MetricTypeId)";

        private const string SetupFuncScript = @"
CREATE FUNCTION TestFunc
(@Description VARCHAR(50),@CreatedDate DateTime)
RETURNS TABLE
AS
  RETURN
    with CharLocations AS (Select OpenParenLoc = CHARINDEX('(',@Description),
         HyphenLoc = CHARINDEX('-',@Description),
         CloseParenLoc = CHARINDEX(')',@Description)),
         SubstringInfos AS (Select StartDateStart = OpenParenLoc + 1,
         StartDateLen = HyphenLoc - OpenParenLoc - 1,
         EndDateStart = HyphenLoc + 1,
         EndDateLen = CloseParenLoc - HyphenLoc - 1
         From CharLocations),
         Substrings As (Select StartDate = SUBSTRING(@Description,StartDateStart,StartDateLen),
         EndDate = SUBSTRING(@Description,EndDateStart,EndDateLen),
         ConcatYear = CASE 
           WHEN (StartDateLen > 5) THEN ''
           ELSE '/' + RIGHT(DATEPART(yyyy,@CreatedDate),2)
         END
         From SubstringInfos)
    (Select StartDate = CONVERT(DATE,StartDate + ConcatYear,1),
    EndDate = CONVERT(DATE,EndDate + ConcatYear,1)
    From Substrings)
";

        private const string SetupProcScript = @"
CREATE PROCEDURE TestProc
(
@dept_name varchar(20)
)
AS
BEGIN
  SELECT * FROM [dbo].[TestTable0]
END";

        private const string SetupRoleScript = @"
CREATE ROLE [TestRole]";

        private const string SetupTrigScript = @"
CREATE TRIGGER TestTrigger ON 
[dbo].[TestTable0]
FOR INSERT
AS
Begin
    SELECT * FROM [dbo].[TestTable0]
End
";

        private const string SetupUserScript = @"
CREATE USER [TestUser] WITHOUT LOGIN WITH DEFAULT_SCHEMA = dbo
";

        private const string SetupViewScript = @"
CREATE VIEW TestView
AS
SELECT * FROM [dbo].[testTable0]
";

        private Database _db;

        [SetUp]
        public void SetUp() {
            _db = new Database("TestAppendComment");
            _db.Connection = ConfigHelper.TestDB.Replace("database=TESTDB", "database=" + _db.Name);
            _db.ExecCreate(true);

            DBHelper.ExecSql(_db.Connection, SetupTable0Script);
            DBHelper.ExecSql(_db.Connection, SetupTable1Script);
            DBHelper.ExecSql(_db.Connection, SetupTableTypeScript);
            DBHelper.ExecSql(_db.Connection, SetupFKScript);
            DBHelper.ExecSql(_db.Connection, SetupFuncScript);
            DBHelper.ExecSql(_db.Connection, SetupProcScript);
            DBHelper.ExecSql(_db.Connection, SetupRoleScript);
            DBHelper.ExecSql(_db.Connection, SetupTrigScript);
            DBHelper.ExecSql(_db.Connection, SetupUserScript);
            DBHelper.ExecSql(_db.Connection, SetupViewScript);
            _db.Dir = _db.Name;
            _db.Load();
            _db.ScriptToDir();
        }

        [TestCase("\\table_types\\", "TYPE_TestTableType.sql")]
        [TestCase("\\foreign_keys\\", "TestTable0.sql")]
        [TestCase("\\functions\\", "TestFunc.sql")]
        [TestCase("\\procedures\\", "TestProc.sql")]
        [TestCase("\\roles\\", "TestRole.sql")]
        [TestCase("\\triggers\\", "TestTrigger.sql")]
        [TestCase("\\tables\\", "TestTable0.sql")]
        [TestCase("\\users\\", "TestUser.sql")]
        [TestCase("\\views\\", "TestView.sql")]
        [TestCase("\\", "schemas.sql")]
        public void TestFilesContainComment(string directory, string fileName) {
            Assert.IsTrue(ValidateFirstLineIncludesComment(_db.Name +
                directory + fileName, Database.AutoGenerateComment));
        }

        [Test]
        public void TestScriptIsValidWithComment() {
            var scriptWithComment = Database.AutoGenerateComment + SetUpTable2Script;
            DBHelper.ExecBatchSql(_db.Connection, scriptWithComment);
        }

        [TearDown]
        public void TearDown() {
            DBHelper.DropDb(_db.Connection);
            DBHelper.ClearPool(_db.Connection);
            var currPath = ".\\TestAppendComment";
            Directory.Delete(currPath, true);
        }

        bool ValidateFirstLineIncludesComment(string filePath, string matchingStr) {
            var firstLine = File.ReadAllLines(filePath)[0];
            return matchingStr.Contains(firstLine);
        }
    }
}
