using System;

namespace SchemaZen.model {
    public class CLRRoutine : INameable, IScriptable {

        public enum CLRRoutineKind
        {
            Procedure,
            TableValuedFunction,
            ScalarFunction
        }

        public string Name { get; set; }
        public string Schema { get; set; }
        public string AssemblyName { get; set; }
        public string AssemblyClass { get; set; }
        public string AssemblyMethod { get; set; }
        public string ExecuteAs { get; set; }
        public CLRRoutineKind RoutineType { get; set; }
        public ColumnList Parameters { get; set; }
        public ColumnList Returns { get; set; }

        public string ScriptCreate() {
            throw new NotImplementedException();
        }
    }
}
