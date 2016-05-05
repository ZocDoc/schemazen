using System.Collections.Generic;
using System.Data.SqlClient;

namespace SchemaZen.model {
    public interface IDatabaseComponent<T> {
        string Name { get; }

        List<T> Load(SqlCommand cmd);
    }
}