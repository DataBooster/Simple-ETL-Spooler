using System;
using System.Data;
using System.Data.Common;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using DbParallel.DataAccess;
using Oracle.DataAccess.Client;

namespace DataBooster.PsInvokeDb.DataAccess
{
    internal static class NamedBindTextCommandExtensions
    {
        public static StoredProcedureResponse ExecuteText(this DbAccess dbAccess, StoredProcedureRequest request)
        {
            if (dbAccess == null)
                throw new ArgumentNullException("dbAccess");
            if (request == null)
                throw new ArgumentNullException("request");
            if (string.IsNullOrWhiteSpace(request.CommandText))
                throw new ArgumentNullException("request.CommandText");
            if (request.CommandType != CommandType.Text)
                throw new NotSupportedException("Only CommandType.Text is supported.");

            StoredProcedureResponse response = new StoredProcedureResponse(dbAccess.DynamicObjectXmlSettings);

            dbAccess.ExecuteReader(
                request.CommandText,
                request.CommandTimeout,
                request.CommandType,
                parameters => AddParameters(parameters, request.InputParameters),
                (reader, resultSetIndex) =>
                {
                    List<BindableDynamicObject> resultSet = new List<BindableDynamicObject>();

                    if (reader.FieldCount > 0)
                        while (reader.Read())
                            resultSet.Add(ReadRow(dbAccess, reader));

                    response.ResultSets.Add(resultSet);
                },
                true);

            if (response.ResultSets.Count == 1 && response.ResultSets[0].Count == 0)
                response.ResultSets.Clear();

            return response;
        }

        private static void AddParameters(DbParameterBuilder parameters, IDictionary<string, object> inputParameters)
        {
            if (parameters.Command is OracleCommand)
                parameters.BindByName = true;

            if (inputParameters == null)
                return;

            foreach (var pair in inputParameters)
            {
                if (string.IsNullOrWhiteSpace(pair.Key))
                    continue;

                DbParameter dbParameter = parameters.Add(pair.Key);
                dbParameter.Value = NormalizeParameterValue(pair.Value);
            }
        }

        private static object NormalizeParameterValue(object value)
        {
            if (value == null)
                return DBNull.Value;

            if (value is IConvertible simpleValue)
                return simpleValue.AsParameterValue();

            if (value is IEnumerable enumerableValue)
            {
                Type elementType = GetEnumerableElementType(value.GetType());

                if (elementType != null)
                    return enumerableValue.AsParameterValue(elementType);
            }

            return value;
        }

        private static Type GetEnumerableElementType(Type type)
        {
            if (type == null || type == typeof(string))
                return null;

            if (type.IsArray)
                return type.GetElementType();

            Type enumerableType = null;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
            {
                enumerableType = type;
            }
            else
            {
                foreach (Type interfaceType in type.GetInterfaces())
                {
                    if (interfaceType.IsGenericType && interfaceType.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                    {
                        enumerableType = interfaceType;
                        break;
                    }
                }
            }

            return enumerableType?.GetGenericArguments()[0];
        }

        private static BindableDynamicObject ReadRow(DbAccess dbAccess, DbDataReader reader)
        {
            IDictionary<string, object> record = new ExpandoObject();

            for (int i = 0; i < reader.FieldCount; i++)
            {
                string fieldName = dbAccess.DynamicPropertyNamingResolver(reader.GetName(i));

                if (string.IsNullOrWhiteSpace(fieldName))
                    fieldName = string.Format(DbAccess.UnnamedColumn_AutonamingFormat, i + 1);

                record[fieldName] = reader.GetColumnValue(i);
            }

            return new BindableDynamicObject(record, dbAccess.DynamicObjectXmlSettings);
        }
    }
}
