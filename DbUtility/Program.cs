using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DbUtility
{
    class Program
    {
        static void Main(string[] args)
        {
            //调用
            DbUtility db = new DbUtility(DbProviderType.SqlServer);
            var dt = db.ExecuteDataTable("select * from Table_A", null);

            IList<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(db.GetParameter(DbProviderType.SqlServer,"@age", "2017-01-06"));
            parameters.Add(db.GetParameter(DbProviderType.SqlServer,"@ID", "8f6018cb-23b4-46d4-8a55-5b2fe545adef"));
            db.ExecuteNonQuery("update Table_A set age=@age where ID=@ID", parameters);

            dt = db.ExecuteDataTable("select * from Table_A", null);

        }
    }
}
