using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Sy.ExpressionBuilder.Attributes;
using Sy.ExpressionBuilder.Expressions;
using Sy.ExpressionBuilder.Modules;
using Sy.ExpressionBuilder.Sql.Dtos;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Sy.ExpressionBuilder.Sql.Expressions;
using Sy.ExpressionBuilder.Sql.Attributes;

namespace Sy.ExpressionBuilder.EFCore.QueryExtensions
{
    /// <summary>
    /// 查询扩展
    /// </summary>
    public static class DbQueryExtensions
    {
        /// <summary>
        /// 获取单个数据
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<T> FirstOrDefaultAsync<T>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new()
        {
            return (await db.ToListAsync<T>(query, parms, commandType)).FirstOrDefault();
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<List<T>> ToListAsync<T>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new()
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            return await command.ToListAsync<T>(query, parms, commandType);
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<List<T>> ToListAsync<T>(this DbCommand command, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new()
        {
            var parameterParms = parms.ToArray();
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            return await GetDataReaderListAsync<T>(reader);
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, List<T2> list2)> ToListAsync<T, T2>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new() where T2 : class, new()
        {
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            return await command.ToListAsync<T, T2>(query, parms, commandType);
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, List<T2> list2)> ToListAsync<T, T2>(this DbCommand command, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new() where T2 : class, new()
        {
            var parameterParms = parms.ToArray();
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            var list = await GetDataReaderListAsync<T>(reader);
            var list2 = new List<T2>();
            if (await reader.NextResultAsync())
                list2 = await GetDataReaderListAsync<T2>(reader);
            return (list, list2);
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, List<T2> list2, List<T3> list3)> ToListAsync<T, T2, T3>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new() where T2 : class, new() where T3 : class, new()
        {
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            return await command.ToListAsync<T, T2, T3>(query, parms, commandType);
        }



        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, List<T2> list2, List<T3> list3)> ToListAsync<T, T2, T3>(this DbCommand command, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure) where T : class, new() where T2 : class, new() where T3 : class, new()
        {
            var parameterParms = parms.ToArray();
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            var list = await GetDataReaderListAsync<T>(reader);
            var list2 = new List<T2>();
            var list3 = new List<T3>();
            if (await reader.NextResultAsync())
                list2 = await GetDataReaderListAsync<T2>(reader);
            if (await reader.NextResultAsync())
                list3 = await GetDataReaderListAsync<T3>(reader);
            return (list, list2, list3);
        }





        /// <summary>
        /// 查询扩展
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, int total)> ToPageAsync<T>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure, ParameterDirection direction = ParameterDirection.Output) where T : class, new()
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            var list = await GetDataReaderListAsync<T>(reader);
            //如果是通过参数返回总数
            if (direction == ParameterDirection.Output)
            {
                var total = await db.Database.GetDbConnection().GetTotalAsync(parameterParms);
                return (list, total);
            }
            return (list, await GetTotalAsync(reader));
        }

        /// <summary>
        /// 查询扩展
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<T> list, List<T2> list2, int total)> ToPageAsync<T, T2>(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure, ParameterDirection direction = ParameterDirection.Output) where T : class, new() where T2 : class, new()
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameterParms);
            var list = new List<T>();
            var list2 = new List<T2>();
            using var reader = await command.ExecuteReaderAsync();
            list = await GetDataReaderListAsync<T>(reader);
            if (await reader.NextResultAsync())
            {
                list2 = await GetDataReaderListAsync<T2>(reader);
                //如果是通过参数返回总数
                if (direction == ParameterDirection.Output)
                {
                    //关闭链接才能获取返回参数
                    if (db.Database.GetDbConnection().State == ConnectionState.Open)
                        await db.Database.CloseConnectionAsync();
                    int total = Convert.ToInt32((parameterParms?.FirstOrDefault(m => m.Direction == ParameterDirection.Output)?.Value ?? 0));
                    return (list, list2, total);
                }
            }
            return (list, list2, await GetTotalAsync(reader));
        }

        /// <summary>
        /// 查询列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<List<PropertyInfoDto>> ToPropertyInfoListAsync(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure)
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            command.DbCommandAssemble(query, commandType, parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            return await GetPropertyInfoListAsync(reader);
        }


        /// <summary>
        /// 获取存储过程结果集模型明细
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<PropertyInfoDto> list, List<PropertyInfoDto> list2)> ToPropertyInfo2ListAsync(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure)
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            command.DbCommandAssemble(query, commandType, parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            var list = await GetPropertyInfoListAsync(reader);
            var list2 = new List<PropertyInfoDto>();
            if (await reader.NextResultAsync())
                list2 = await GetDataReaderListAsync<PropertyInfoDto>(reader);
            return (list, list2);
        }



        /// <summary>
        /// 获取存储过程结果集模型明细
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="db"></param>
        /// <param name="query"></param>
        /// <returns></returns>
        public static async Task<(List<PropertyInfoDto> list, List<PropertyInfoDto> list2, List<PropertyInfoDto> list3)> ToPropertyInfo3ListAsync(this DbContext db, string query, List<SqlParameter> parms, CommandType commandType = CommandType.StoredProcedure)
        {
            var parameterParms = parms.ToArray();
            await db.EnsureOpenAsync();
            using var command = db.Database.GetDbConnection().CreateCommand();
            command.DbCommandAssemble(query, commandType, parameterParms);
            using var reader = await command.ExecuteReaderAsync();
            var list = await GetPropertyInfoListAsync(reader);
            var list2 = new List<PropertyInfoDto>();
            var list3 = new List<PropertyInfoDto>();
            if (await reader.NextResultAsync())
                list2 = await GetDataReaderListAsync<PropertyInfoDto>(reader);
            if (await reader.NextResultAsync())
                list3 = await GetDataReaderListAsync<PropertyInfoDto>(reader);
            return (list, list2, list3);
        }

        /// <summary>
        /// 获取ExecuteReader返回的数据列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        private static async Task<List<T>> GetDataReaderListAsync<T>(DbDataReader reader) where T : class, new()
        {
            var list = new List<T>();
            var listColumns = new T().GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(m => m.MemberType == MemberTypes.Property).ToList();
            while (await reader.ReadAsync())
            {
                var newObject = new T();
                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var name = reader.GetName(i);
                    PropertyInfo prop = listColumns.FirstOrDefault(a => a.Name.ToLower().Equals(name.ToLower()));
                    //类型不对应 ，直接返回
                    if (prop == null)
                        continue;
                    var isNullType = ExpressionExtension.IsNullableType(prop.PropertyType);
                    var val = reader.IsDBNull(i) ? null : reader[i];
                    //值为空，并且是可空类型 
                    if (val == null && isNullType)
                        continue;
                    if (isNullType)
                    {
                        if (prop.PropertyType.GetGenericArguments()[0].IsEnum)
                        {
                            var enumValue = Enum.Parse(prop.PropertyType.GetGenericArguments()[0], val.ToString());
                            prop.SetValue(newObject, enumValue, null);
                        }
                        else
                            prop.SetValue(newObject, Convert.ChangeType(val, prop.PropertyType), null);
                    }
                    else
                    {
                        if (prop.PropertyType.IsValueType && prop.PropertyType.IsGenericType && prop.PropertyType.IsEnum)
                        {
                            var enumValue = Enum.Parse(prop.PropertyType.GetGenericArguments()[0], val.ToString());
                            prop.SetValue(newObject, enumValue, null);
                        }
                        else
                            prop.SetValue(newObject, Convert.ChangeType(val, prop.PropertyType), null);
                    }
                }
                list.Add(newObject);
            }
            return list;
        }


        /// <summary>
        /// 获取ExecuteReader返回的数据列表
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        private static async Task<List<PropertyInfoDto>> GetPropertyInfoListAsync(DbDataReader reader)
        {
            var propertyInfos = new List<PropertyInfoDto>();
            var schemaTable = await reader.GetSchemaTableAsync();
            foreach (DataRow row in schemaTable.Rows)
            {
                var property = new PropertyInfoDto();
                property.PropertyName = row["ColumnName"].ToString();
                property.PropertyType = (Type)row["DataType"];
                property.IsNull = property.PropertyType.IsNull();
                property.TypeName = property?.PropertyType?.Name?.MapCsharpSimpleType();
                propertyInfos.Add(property);
            }
            return propertyInfos;
        }


        /// <summary>
        ///通过列表返回总行数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        private static async Task<int> GetTotalAsync(this DbConnection connection, SqlParameter[] parameters)
        {
            //关闭链接才能获取返回参数
            if (connection.State == ConnectionState.Open)
                await connection.CloseAsync();
            return Convert.ToInt32((parameters?.FirstOrDefault(m => m.Direction == ParameterDirection.Output)?.Value ?? 0));
        }

        /// <summary>
        ///通过列表返回总行数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        private static async Task<int> GetTotalAsync(this SqlConnection connection, SqlParameter[] parameters)
        {
            //关闭链接才能获取返回参数
            if (connection.State == ConnectionState.Open)
                await connection.CloseAsync();
            return Convert.ToInt32((parameters?.FirstOrDefault(m => m.Direction == ParameterDirection.Output)?.Value ?? 0));
        }

        /// <summary>
        ///通过列表返回总行数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="command"></param>
        /// <returns></returns>
        private static async Task<int> GetTotalAsync(DbDataReader reader)
        {
            // 读取总行数
            if (await reader.NextResultAsync())
            {
                reader.Read();
                return reader.GetInt32(0); // TotalRows字段是第一个字段
            }
            return 0;
        }


        /// <summary>
        /// 组装DbCommand
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private static DbCommand DbCommandAssemble(this DbCommand command, string query, CommandType commandType, SqlParameter[] parameters)
        {
            command.CommandText = query;
            command.CommandType = commandType;
            command.Parameters.AddRange(parameters);
            return command;
        }


        /// <summary>
        /// 确保打开数据库
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private static async Task EnsureOpenAsync(this DbContext db)
        {
            await db.Database.GetDbConnection().EnsureOpenAsync();
        }

        /// <summary>
        /// 确保打开数据库
        /// </summary>
        /// <param name="db"></param>
        /// <returns></returns>
        private static async Task EnsureOpenAsync(this DbConnection dbConnection)
        {
            if (dbConnection.State == ConnectionState.Closed)
                await dbConnection.OpenAsync();
            if (dbConnection.State == ConnectionState.Broken)
            {
                await dbConnection.CloseAsync();
                await dbConnection.OpenAsync();
            }
        }


        /// <summary>
        ///  获取存储过程参数
        /// </summary>
        /// <returns></returns>
        public static List<SqlParameter> GetSqlParameters<Input>(this Input input) where Input : class, IQueryBase
        {
            List<SqlParameter> parameters = new List<SqlParameter>();
            var properties = input.GetType().GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(m => m.MemberType == MemberTypes.Property).ToList();
            if (properties?.Any() ?? false)
            {
                foreach (PropertyInfo pi in properties)
                {
                    var output = pi.GetCustomAttribute<OutputAttribute>();
                    var notQuery = pi.GetCustomAttribute<NotQueryAttribute>();
                    if (notQuery == null)
                    {
                        var value = pi.GetValue(input);
                        if (output == null)
                        {
                            //空值，并且是可空的 
                            if (value == null && (pi.PropertyType.IsGenericType && pi.PropertyType.GetGenericTypeDefinition() == typeof(Nullable<>)))
                                parameters.Add(new SqlParameter(pi.Name, DBNull.Value));
                            else
                                parameters.Add(new SqlParameter(pi.Name, value));
                        }
                        else
                        {
                            SqlParameter param = new SqlParameter(pi.Name, output.SqlDbType);
                            param.Direction = ParameterDirection.Output;
                            parameters.Add(param);
                        }
                    }
                }
            }
            return parameters;
        }
    }
}
