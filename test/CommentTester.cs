﻿using System;
using System.IO;
using NUnit.Framework;
using SchemaZen.Library;
using SchemaZen.Library.Models;

namespace SchemaZen.Tests
{
    [TestFixture]
    class CommentTester {
        private string _comment;
        private Database _db;

        [SetUp]
        public void SetUp() {
            _db = new Database("TestAppendComment");
            _db.Connection = ConfigHelper.TestDB.Replace("database=TESTDB", "database=" + _db.Name);
            _db.ExecCreate(true);
            _comment = String.Format(
                @"//----------------------------------------------------------------------------
             // <auto-generated>
             //     This file was generated by schemazen.
             //     Date: {0}
             //
             //     Changes to this file may cause incorrect behavior and will be lost if
             //     the code is regenerated.
             // </auto-generated>
             //----------------------------------------------------------------------------",
                DateTime.Today.ToString("MM-dd-yyyy"));

            DBHelper.ExecSql(_db.Connection, TestStrings.SetupTable0Script);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupTable1Script);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupTableTypeScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupFKScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupFuncScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupProcScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupRoleScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupTrigScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupUserScript);
            DBHelper.ExecSql(_db.Connection, TestStrings.SetupViewScript);
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
        public void TestFilesContainComment(string directory, string fileName)
        {
            Assert.IsTrue(ValidateFirstLineIncludesComment(_db.Name +
                directory + fileName, _comment));
        }

        [TearDown]
        public void TearDown() {
            DBHelper.DropDb(_db.Connection);
            DBHelper.ClearPool(_db.Connection);
        }

        bool ValidateFirstLineIncludesComment(string filePath, string matchingStr) {
            var firstLine = File.ReadAllLines(filePath)[0];
            return matchingStr.Contains(firstLine);
        }
    }
}
