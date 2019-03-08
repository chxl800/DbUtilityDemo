using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Common;
using System.Reflection;

namespace DbUtility
{
    /// <summary> 
    /// 通用数据库访问类，封装了对数据库的常见操作 
    /// </summary> 
    public sealed class DbUtility
    {
        public static readonly string constr = ConfigurationManager.AppSettings["DBTest"].ToString();
        public string ConnectionString { get; set; }
        private DbProviderFactory providerFactory;
        /// <summary> 
        /// 构造函数 
        /// </summary> 
        /// <param name="connectionString">数据库连接字符串</param> 
        /// <param name="providerType">数据库类型枚举，参见<paramref name="providerType"/></param> 
        public DbUtility(DbProviderType providerType)
        {
            ConnectionString = constr;
            providerFactory = ProviderFactory.GetDbProviderFactory(providerType);
            if (providerFactory == null)
            {
                throw new ArgumentException("Can't load DbProviderFactory for given value of providerType");
            }
        }
        /// <summary>    
        /// 对数据库执行增删改操作，返回受影响的行数。    
        /// </summary>    
        /// <param name="sql">要执行的增删改的SQL语句</param>    
        /// <param name="parameters">执行增删改语句所需要的参数</param> 
        /// <returns></returns>   
        public int ExecuteNonQuery(string sql, IList<DbParameter> parameters)
        {
            return ExecuteNonQuery(sql, parameters, CommandType.Text);
        }
        /// <summary>    
        /// 对数据库执行增删改操作，返回受影响的行数。    
        /// </summary>    
        /// <param name="sql">要执行的增删改的SQL语句</param>    
        /// <param name="parameters">执行增删改语句所需要的参数</param> 
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns> 
        public int ExecuteNonQuery(string sql, IList<DbParameter> parameters, CommandType commandType)
        {
            using (DbCommand command = CreateDbCommand(sql, parameters, commandType))
            {
                command.Connection.Open();
                int affectedRows = command.ExecuteNonQuery();
                command.Connection.Close();
                return affectedRows;
            }
        }

        /// <summary>    
        /// 执行一个查询语句，返回一个关联的DataReader实例    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <returns></returns>  
        public DbDataReader ExecuteReader(string sql, IList<DbParameter> parameters)
        {
            return ExecuteReader(sql, parameters, CommandType.Text);
        }

        /// <summary>    
        /// 执行一个查询语句，返回一个关联的DataReader实例    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns>  
        public DbDataReader ExecuteReader(string sql, IList<DbParameter> parameters, CommandType commandType)
        {
            DbCommand command = CreateDbCommand(sql, parameters, commandType);
            command.Connection.Open();
            return command.ExecuteReader(CommandBehavior.CloseConnection);
        }

        /// <summary>    
        /// 执行一个查询语句，返回一个包含查询结果的DataTable    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <returns></returns> 
        public DataTable ExecuteDataTable(string sql, IList<DbParameter> parameters)
        {
            return ExecuteDataTable(sql, parameters, CommandType.Text);
        }
        /// <summary>    
        /// 执行一个查询语句，返回一个包含查询结果的DataTable    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns> 
        public DataTable ExecuteDataTable(string sql, IList<DbParameter> parameters, CommandType commandType)
        {
            using (DbCommand command = CreateDbCommand(sql, parameters, commandType))
            {
                using (DbDataAdapter adapter = providerFactory.CreateDataAdapter())
                {
                    adapter.SelectCommand = command;
                    DataTable data = new DataTable();
                    adapter.Fill(data);
                    return data;
                }
            }
        }

        /// <summary>    
        /// 执行一个查询语句，返回查询结果的第一行第一列    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>    
        /// <returns></returns>    
        public Object ExecuteScalar(string sql, IList<DbParameter> parameters)
        {
            return ExecuteScalar(sql, parameters, CommandType.Text);
        }

        /// <summary>    
        /// 执行一个查询语句，返回查询结果的第一行第一列    
        /// </summary>    
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>    
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns>    
        public Object ExecuteScalar(string sql, IList<DbParameter> parameters, CommandType commandType)
        {
            using (DbCommand command = CreateDbCommand(sql, parameters, commandType))
            {
                command.Connection.Open();
                object result = command.ExecuteScalar();
                command.Connection.Close();
                return result;
            }
        }

        /// <summary> 
        /// 查询多个实体集合 
        /// </summary> 
        /// <typeparam name="T">返回的实体集合类型</typeparam> 
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <returns></returns> 
        public List<T> QueryForList<T>(string sql, IList<DbParameter> parameters) where T : new()
        {
            return QueryForList<T>(sql, parameters, CommandType.Text);
        }

        /// <summary> 
        ///  查询多个实体集合 
        /// </summary> 
        /// <typeparam name="T">返回的实体集合类型</typeparam> 
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>    
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns> 
        public List<T> QueryForList<T>(string sql, IList<DbParameter> parameters, CommandType commandType) where T : new()
        {
            DataTable data = ExecuteDataTable(sql, parameters, commandType);
            return EntityChange.GetEntities<T>(data);
        }
      
        /// <summary> 
        /// 查询单个实体 
        /// </summary> 
        /// <typeparam name="T">返回的实体集合类型</typeparam> 
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <returns></returns> 
        public T QueryForObject<T>(string sql, IList<DbParameter> parameters) where T : new()
        {
            return QueryForObject<T>(sql, parameters, CommandType.Text);
        }

        /// <summary> 
        /// 查询单个实体 
        /// </summary> 
        /// <typeparam name="T">返回的实体集合类型</typeparam> 
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param>    
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns> 
        public T QueryForObject<T>(string sql, IList<DbParameter> parameters, CommandType commandType) where T : new()
        {
            return QueryForList<T>(sql, parameters, commandType)[0];
        }

        public DbParameter CreateDbParameter(string name, object value)
        {
            return CreateDbParameter(name, ParameterDirection.Input, value);
        }

        public DbParameter CreateDbParameter(string name, ParameterDirection parameterDirection, object value)
        {
            DbParameter parameter = providerFactory.CreateParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            parameter.Direction = parameterDirection;
            return parameter;
        }

        /// <summary> 
        /// 创建一个DbCommand对象 
        /// </summary> 
        /// <param name="sql">要执行的查询语句</param>    
        /// <param name="parameters">执行SQL查询语句所需要的参数</param> 
        /// <param name="commandType">执行的SQL语句的类型</param> 
        /// <returns></returns> 
        private DbCommand CreateDbCommand(string sql, IList<DbParameter> parameters, CommandType commandType)
        {
            DbConnection connection = providerFactory.CreateConnection();
            DbCommand command = providerFactory.CreateCommand();
            connection.ConnectionString = ConnectionString;
            command.CommandText = sql;
            command.CommandType = commandType;
            command.Connection = connection;
            if (!(parameters == null || parameters.Count == 0))
            {
                foreach (DbParameter parameter in parameters)
                {
                    command.Parameters.Add(parameter);
                }
            }
            return command;
        }

        /// <summary>
        /// SqlParameter
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public DbParameter GetParameter(DbProviderType providerType,string name, object value)
        {
            if (value == null)
                value = System.DBNull.Value;

            if(providerType== DbProviderType.SqlServer)
                return new System.Data.SqlClient.SqlParameter(name, value);
            else if (providerType == DbProviderType.OleDb)
                return new System.Data.OleDb.OleDbParameter(name, value);
            else
                return new System.Data.SqlClient.SqlParameter(name, value);
       //System.Data.Odbc.OdbcParameter
       //System.Data.OleDb.OleDbParameter
       //System.Data.OracleClient.OracleParameter
       //System.Data.SqlClient.SqlParameter
       //System.Data.SqlServerCe.SqlCeParameter
        }

    }

    /// <summary> 
    /// DbProviderFactory工厂类 
    /// </summary> 
    public class ProviderFactory
    {
        private static Dictionary<DbProviderType, string> providerInvariantNames = new Dictionary<DbProviderType, string>();
        private static Dictionary<DbProviderType, DbProviderFactory> providerFactoies = new Dictionary<DbProviderType, DbProviderFactory>(20);
        static ProviderFactory()
        {
            //加载已知的数据库访问类的程序集 
            providerInvariantNames.Add(DbProviderType.SqlServer, "System.Data.SqlClient");
            providerInvariantNames.Add(DbProviderType.OleDb, "System.Data.OleDb");
            providerInvariantNames.Add(DbProviderType.ODBC, "System.Data.ODBC");
            providerInvariantNames.Add(DbProviderType.Oracle, "Oracle.DataAccess.Client");
            providerInvariantNames.Add(DbProviderType.MySql, "MySql.Data.MySqlClient");
            providerInvariantNames.Add(DbProviderType.SQLite, "System.Data.SQLite");
            providerInvariantNames.Add(DbProviderType.Firebird, "FirebirdSql.Data.Firebird");
            providerInvariantNames.Add(DbProviderType.PostgreSql, "Npgsql");
            providerInvariantNames.Add(DbProviderType.DB2, "IBM.Data.DB2.iSeries");
            providerInvariantNames.Add(DbProviderType.Informix, "IBM.Data.Informix");
            providerInvariantNames.Add(DbProviderType.SqlServerCe, "System.Data.SqlServerCe");
        }
        /// <summary> 
        /// 获取指定数据库类型对应的程序集名称 
        /// </summary> 
        /// <param name="providerType">数据库类型枚举</param> 
        /// <returns></returns> 
        public static string GetProviderInvariantName(DbProviderType providerType)
        {
            return providerInvariantNames[providerType];
        }
        /// <summary> 
        /// 获取指定类型的数据库对应的DbProviderFactory 
        /// </summary> 
        /// <param name="providerType">数据库类型枚举</param> 
        /// <returns></returns> 
        public static DbProviderFactory GetDbProviderFactory(DbProviderType providerType)
        {
            //如果还没有加载，则加载该DbProviderFactory 
            if (!providerFactoies.ContainsKey(providerType))
            {
                providerFactoies.Add(providerType, ImportDbProviderFactory(providerType));
            }
            return providerFactoies[providerType];
        }
        /// <summary> 
        /// 加载指定数据库类型的DbProviderFactory 
        /// </summary> 
        /// <param name="providerType">数据库类型枚举</param> 
        /// <returns></returns> 
        private static DbProviderFactory ImportDbProviderFactory(DbProviderType providerType)
        {
            string providerName = providerInvariantNames[providerType];
            DbProviderFactory factory = null;
            try
            {
                //从全局程序集中查找 
                factory = DbProviderFactories.GetFactory(providerName);
            }
            catch (ArgumentException e)
            {
                factory = null;
            }
            return factory;
        }
    }



    /// <summary> 
    /// 数据库类型枚举 
    /// </summary> 
    public enum DbProviderType : byte
    {
        SqlServer,
        MySql,
        SQLite,
        Oracle,
        ODBC,
        OleDb,
        Firebird,
        PostgreSql,
        DB2,
        Informix,
        SqlServerCe
    }
    /// <summary>
    /// 实体转化
    /// </summary>
    public class EntityChange
    {
        /// <summary> 
        /// 利用反射将DataTable转换为List<T>对象
        /// </summary> 
        /// <param name="dt">DataTable 对象</param> 
        /// <returns>List<T>集合</returns> 
        public static List<T> GetEntities<T>(DataTable dt) where T : new()
        {
            // 定义集合 
            List<T> ts = new List<T>();
            //定义一个临时变量 
            string tempName = string.Empty;
            //遍历DataTable中所有的数据行 
            foreach (DataRow dr in dt.Rows)
            {
                T t = new T();
                // 获得此模型的公共属性 
                PropertyInfo[] propertys = t.GetType().GetProperties();
                //遍历该对象的所有属性 
                foreach (PropertyInfo pi in propertys)
                {
                    tempName = pi.Name;//将属性名称赋值给临时变量 
                                       //检查DataTable是否包含此列（列名==对象的属性名）  
                    if (dt.Columns.Contains(tempName))
                    {
                        //取值 
                        object value = dr[tempName];
                        //如果非空，则赋给对象的属性 
                        if (value != DBNull.Value)
                        {
                            pi.SetValue(t, value, null);
                        }
                    }
                }
                //对象添加到泛型集合中 
                ts.Add(t);
            }
            return ts;
        }


        /// <summary> 
        /// 利用反射将DataRow转换为T对象
        /// </summary> 
        /// <param name="dr">DataRow 转换为T对象</param> 
        /// <returns>T</returns> 
        public static T GetEntitie<T>(DataRow dr) where T : new()
        {
            //定义一个临时变量 
            string tempName = string.Empty;
            T t = new T();
            // 获得此模型的公共属性 
            PropertyInfo[] propertys = t.GetType().GetProperties();
            //遍历该对象的所有属性 
            foreach (PropertyInfo pi in propertys)
            {
                tempName = pi.Name;//将属性名称赋值给临时变量 
                                   //检查DataTable是否包含此列（列名==对象的属性名）  
                if (dr.Table.Columns.Contains(tempName))
                {
                    //取值 
                    object value = dr[tempName];
                    //如果非空，则赋给对象的属性 
                    if (value != DBNull.Value)
                    {
                        pi.SetValue(t, value, null);
                    }
                }
            }
            //对象添加到泛型集合中 
 
            return t;
        }
        /// <summary>
        /// IDataReader 转T实体
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="reader"></param>
        /// <returns></returns>
        private static T GetEntitie<T>(IDataReader reader) where T : new()
        {
            T obj = default(T);
            try
            {
                Type type = typeof(T);
                obj = (T)Activator.CreateInstance(type);//从当前程序集里面通过反射的方式创建指定类型的对象   
                PropertyInfo[] propertyInfos = type.GetProperties();//获取指定类型里面的所有属性
                foreach (PropertyInfo propertyInfo in propertyInfos)
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        string fieldName = reader.GetName(i);
                        if (fieldName == propertyInfo.Name)
                        {
                            object val = reader[fieldName];//读取表中某一条记录里面的某一列
                            if (val != DBNull.Value)
                            {
                                propertyInfo.SetValue(obj, val,null);
                            }
                            break;
                        }
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return obj;
        }
    }
}