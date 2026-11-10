using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Globalization;
using System.Runtime.ExceptionServices;
using System.ServiceModel.Channels;
using Newtonsoft.Json;
using Snowflake.Data.Client;
using DataBooster.PsConvertToOracleInput;

namespace DataBooster.Simple_ETL_Spooler.InvokeSnowflake
{
    public class SnowflakeClient : IDisposable
    {
        private class SourceConfig
        {
            public string TplKey { get; set; }
            public string OvrParams { get; set; } = "{}";    // Default to empty JSON object if not specified
        }
        private SnowflakeDbConnection _connection;

        internal class SnowflakeConnException : Exception
        {
            internal SnowflakeConnException(string message, Exception innerException)
                : base(message, innerException) { }
        }

        public SnowflakeClient(string connectionString)
        {
            _connection = string.IsNullOrWhiteSpace(connectionString) ? throw new ArgumentNullException(nameof(connectionString)) : new SnowflakeDbConnection(connectionString);
        }

        public void Enter()
        {
            SnowflakeDbConnection conn = _connection ?? throw new ObjectDisposedException(nameof(SnowflakeClient));

            try
            {
                switch (conn.State)
                {
                    case ConnectionState.Closed:
                        conn.Open();
                        break;
                    case ConnectionState.Broken:
                        conn.Close();
                        conn.Open();
                        break;
                }
            }
            catch (Exception ex)
            {
                throw new SnowflakeConnException("Error connecting to Snowflake.", ex);
            }
        }

        private void CloseConnection(bool swallowExceptions, bool disposeConnection)
        {
            SnowflakeDbConnection conn = _connection;

            if (disposeConnection)
                _connection = null;

            if (conn == null)
                return;

            try
            {
                if (conn.State != ConnectionState.Closed)
                    conn.Close();
            }
            catch when (swallowExceptions)
            {
            }

            if (!disposeConnection)
                return;

            try
            {
                conn.Dispose();
            }
            catch when (swallowExceptions)
            {
            }
        }

        public void Exit()
        {
            CloseConnection(false, false);
        }

        public void Dispose()
        {
            CloseConnection(true, true);
        }

        public OrderedDictionary Execute(string sfQuery, IDictionary<string, object> bindParams = null, IDictionary<string, string> columnMapping = null, int timeoutSeconds = 0)
        {
            string MapColumnToParam(string columnName)
            {
                if (columnMapping != null && columnMapping.TryGetValue(columnName, out string toName))
                    return toName;
                else
                    return columnName;
            }

            if (string.IsNullOrWhiteSpace(sfQuery))
                throw new ArgumentException("Value cannot be null or whitespace.", nameof(sfQuery));

            Enter();
            OrderedDictionary resultDictionary;
            IReadOnlyList<object> paramValues = TryRewriteNamedBindParameters(sfQuery, bindParams, out string commandText);

            using (var cmd = new SnowflakeDbCommand(_connection, commandText))
            {
                cmd.CommandType = CommandType.Text;
                if (timeoutSeconds > 0)
                    cmd.CommandTimeout = timeoutSeconds;

                for (int i = 0; i < paramValues.Count; i++)
                {
                    object parameterValue = NormalizeParameterValue(paramValues[i], out DbType parameterDbType);
                    var parameter = cmd.CreateParameter();
                    parameter.ParameterName = (i + 1).ToString(CultureInfo.InvariantCulture);
                    parameter.DbType = parameterDbType;
                    parameter.Value = parameterValue ?? DBNull.Value;
                    cmd.Parameters.Add(parameter);
                }

                using (var reader = cmd.ExecuteReader())
                {
                    string columnName, paramName;
                    resultDictionary = new OrderedDictionary(reader.FieldCount, StringComparer.OrdinalIgnoreCase);
                    if (reader.FieldCount > 0)
                    {
                        var resultAssocArrays = new List<object>[reader.FieldCount];
                        for (int col = 0; col < reader.FieldCount; col++)
                        {
                            resultAssocArrays[col] = new List<object>();
                        }

                        while (reader.Read())
                        {
                            for (int col = 0; col < reader.FieldCount; col++)
                            {
                                resultAssocArrays[col].Add(GetSfValue(reader, col));
                            }
                        }

                        if (resultAssocArrays[0].Count > 0)
                        {
                            for (int col = 0; col < reader.FieldCount; col++)
                            {
                                columnName = reader.GetName(col);
                                paramName = MapColumnToParam(columnName);
                                if (!string.IsNullOrWhiteSpace(paramName))
                                    resultDictionary[paramName] = resultAssocArrays[col].ToArray();
                            }
                        }
                    }
                }
            }

            return resultDictionary;
        }

        // Snowflake ADO.NET binding is positional rather than true named binding.
        // This method scans SQL text, rewrites supported pseudo named placeholders
        // such as :paramName into ordinal placeholders (:1, :2, ...), and returns
        // the parameter values in the same ordinal order. Placeholders inside common
        // SQL literal/comment forms are skipped so only executable SQL text is rewritten.
        private IReadOnlyList<object> TryRewriteNamedBindParameters(string queryText, IDictionary<string, object> inputParams, out string rewrittenQuery)
        {
            if (inputParams == null || inputParams.Count == 0 || queryText.IndexOf(':') < 0)
            {
                rewrittenQuery = queryText;
                return Array.Empty<object>();
            }

            var rewrittenText = new StringBuilder(queryText.Length);
            var values = new List<object>(inputParams.Count);
            var ordinalByParameterName = new Dictionary<string, int>(inputParams.Count, StringComparer.OrdinalIgnoreCase);

            bool IsParameterNameStartChar(char ch)
            {
                return char.IsLetter(ch) || ch == '_';
            }

            bool IsParameterNameChar(char ch)
            {
                return char.IsLetterOrDigit(ch) || ch == '_';
            }

            bool IsNamedParameterStartBoundary(int chIndex)
            {
                if (chIndex <= 0)
                    return true;

                char previousChar = queryText[chIndex - 1];
                return !(IsParameterNameChar(previousChar) || previousChar == ':' || previousChar == '.' || previousChar == '$' || previousChar == ']' || previousChar == ')');
            }

            bool IsNamedParameterEndBoundary(int chIndex)
            {
                if (chIndex >= queryText.Length)
                    return true;

                char nextChar = queryText[chIndex];
                if (char.IsWhiteSpace(nextChar))
                    return true;

                switch (nextChar)
                {
                    case ',':
                    case ')':
                    case ';':
                    case '+':
                    case '-':
                    case '*':
                    case '/':
                    case '%':
                    case '=':
                    case '<':
                    case '>':
                    case '!':
                    case '|':
                    case '&':
                    case '^':
                    case '~':
                    case ']':
                    case '}':
                        return true;
                    case ':':
                        return chIndex + 1 < queryText.Length && queryText[chIndex + 1] == ':';
                    default:
                        return false;
                }
            }

            bool TryReadDollarQuotedLiteral(int startIndex, out int endIndexExclusive)
            {
                endIndexExclusive = startIndex;
                if (queryText[startIndex] != '$')
                    return false;

                int tagEndIndex = queryText.IndexOf('$', startIndex + 1);
                if (tagEndIndex < 0)
                    return false;

                for (int i = startIndex + 1; i < tagEndIndex; i++)
                {
                    if (!IsParameterNameChar(queryText[i]))
                        return false;
                }

                string delimiter = queryText.Substring(startIndex, tagEndIndex - startIndex + 1);
                int closingDelimiterIndex = queryText.IndexOf(delimiter, tagEndIndex + 1, StringComparison.Ordinal);
                endIndexExclusive = closingDelimiterIndex >= 0 ? closingDelimiterIndex + delimiter.Length : queryText.Length;
                return true;
            }

            bool TryReadPseudoNamedParameter(int startIndex, out string parameterName, out int endIndexExclusive)
            {
                parameterName = null;
                endIndexExclusive = startIndex;

                if (startIndex < 0 || startIndex >= queryText.Length)
                    return false;

                if (queryText[startIndex] != ':')
                    return false;

                if (!IsNamedParameterStartBoundary(startIndex))
                    return false;

                int nameStartIndex = startIndex + 1;
                if (nameStartIndex >= queryText.Length)
                    return false;

                if (queryText[nameStartIndex] == ':')
                    return false;

                char firstNameChar = queryText[nameStartIndex];
                if (!IsParameterNameStartChar(firstNameChar))
                    return false;

                int index = nameStartIndex + 1;
                while (index < queryText.Length && IsParameterNameChar(queryText[index]))
                {
                    index++;
                }

                if (!IsNamedParameterEndBoundary(index))
                    return false;

                parameterName = queryText.Substring(nameStartIndex, index - nameStartIndex);
                endIndexExclusive = index;
                return true;
            }

            void AppendSegment(int startIndex, int endIndexExclusive)
            {
                rewrittenText.Append(queryText, startIndex, endIndexExclusive - startIndex);
            }

            bool TrySkipEscapedDelimitedText(ref int chIndex, char delimiter)
            {
                if (queryText[chIndex] != delimiter)
                    return false;

                int endIndex = chIndex + 1;
                while (endIndex < queryText.Length)
                {
                    if (queryText[endIndex] == delimiter)
                    {
                        endIndex++;
                        if (endIndex < queryText.Length && queryText[endIndex] == delimiter)
                            endIndex++;
                        else
                            break;
                    }
                    else
                    {
                        endIndex++;
                    }
                }

                AppendSegment(chIndex, endIndex);
                chIndex = endIndex;
                return true;
            }

            bool TrySkipLineComment(ref int chIndex)
            {
                if (!(queryText[chIndex] == '-' && chIndex + 1 < queryText.Length && queryText[chIndex + 1] == '-'))
                    return false;

                int endIndex = chIndex + 2;
                while (endIndex < queryText.Length && queryText[endIndex] != '\r' && queryText[endIndex] != '\n')
                    endIndex++;

                AppendSegment(chIndex, endIndex);
                chIndex = endIndex;
                return true;
            }

            bool TrySkipBlockComment(ref int chIndex)
            {
                if (!(queryText[chIndex] == '/' && chIndex + 1 < queryText.Length && queryText[chIndex + 1] == '*'))
                    return false;

                int endIndex = queryText.IndexOf("*/", chIndex + 2, StringComparison.Ordinal);
                endIndex = endIndex >= 0 ? endIndex + 2 : queryText.Length;

                AppendSegment(chIndex, endIndex);
                chIndex = endIndex;
                return true;
            }

            bool TrySkipDollarQuotedLiteral(ref int chIndex)
            {
                if (queryText[chIndex] != '$' || !TryReadDollarQuotedLiteral(chIndex, out int endIndex))
                    return false;

                AppendSegment(chIndex, endIndex);
                chIndex = endIndex;
                return true;
            }

            bool TryRewriteNamedParameter(ref int chIndex)
            {
                if (!TryReadPseudoNamedParameter(chIndex, out string parameterName, out int parameterEndIndex))
                    return false;

                if (!inputParams.TryGetValue(parameterName, out object parameterValue))
                {
                    AppendSegment(chIndex, parameterEndIndex);
                    chIndex = parameterEndIndex;
                    return true;
                }

                if (!ordinalByParameterName.TryGetValue(parameterName, out int ordinal))
                {
                    ordinal = values.Count + 1;
                    ordinalByParameterName.Add(parameterName, ordinal);
                    values.Add(parameterValue);
                }

                rewrittenText.Append(':').Append(ordinal.ToString(CultureInfo.InvariantCulture));
                chIndex = parameterEndIndex;
                return true;
            }

            for (int chIndex = 0; chIndex < queryText.Length;)
            {
                if (TrySkipLineComment(ref chIndex)
                    || TrySkipBlockComment(ref chIndex)
                    || TrySkipEscapedDelimitedText(ref chIndex, '\'')   // Single-quoted string literal
                    || TrySkipEscapedDelimitedText(ref chIndex, '"')    // Double-quoted identifier
                    || TrySkipDollarQuotedLiteral(ref chIndex)
                    || TryRewriteNamedParameter(ref chIndex))
                    continue;

                rewrittenText.Append(queryText[chIndex]);
                chIndex++;
            }

            rewrittenQuery = (values.Count == 0) ? queryText : rewrittenText.ToString();
            return values;
        }

        // Snowflake bind values arrive through a dynamic object dictionary, so the
        // CLR value needs to be normalized into a stable provider-friendly form and
        // paired with an explicit DbType before being added to SnowflakeDbParameter.
        // Simple scalar types are preserved, selected CLR-specific values are mapped
        // to predictable string/date representations, and complex objects/custom
        // structs are serialized to compact JSON text so they can still be passed
        // through Snowflake's positional binding model.
        private static object NormalizeParameterValue(object value, out DbType dbType)
        {
            if (value == null || value == DBNull.Value)
            {
                dbType = DbType.Object;
                return null;
            }

            Type valueType = value.GetType();
            Type underlyingType = Nullable.GetUnderlyingType(valueType) ?? valueType;

            if (underlyingType.IsEnum)
            {
                underlyingType = Enum.GetUnderlyingType(underlyingType);
                value = Convert.ChangeType(value, underlyingType, CultureInfo.InvariantCulture);
            }

            switch (Type.GetTypeCode(underlyingType))
            {
                case TypeCode.Boolean:
                    dbType = DbType.Boolean;
                    return value;
                case TypeCode.Byte:
                    dbType = DbType.Byte;
                    return value;
                case TypeCode.SByte:
                    dbType = DbType.SByte;
                    return value;
                case TypeCode.Int16:
                    dbType = DbType.Int16;
                    return value;
                case TypeCode.UInt16:
                    dbType = DbType.UInt16;
                    return value;
                case TypeCode.Int32:
                    dbType = DbType.Int32;
                    return value;
                case TypeCode.UInt32:
                    dbType = DbType.UInt32;
                    return value;
                case TypeCode.Int64:
                    dbType = DbType.Int64;
                    return value;
                case TypeCode.UInt64:
                    dbType = DbType.UInt64;
                    return value;
                case TypeCode.Single:
                    dbType = DbType.Single;
                    return value;
                case TypeCode.Double:
                    dbType = DbType.Double;
                    return value;
                case TypeCode.Decimal:
                    dbType = DbType.Decimal;
                    return value;
                case TypeCode.DateTime:
                    dbType = DbType.DateTime2;
                    return value;
                case TypeCode.String:
                    dbType = DbType.String;
                    return value;
                case TypeCode.Char:
                    dbType = DbType.String;
                    return value.ToString();
            }

            switch (underlyingType)
            {
                case Type _ when underlyingType == typeof(char[]):
                    dbType = DbType.String;
                    return new string((char[])value);
                case Type _ when underlyingType == typeof(byte[]):
                    dbType = DbType.Binary;
                    return value;
                case Type _ when underlyingType == typeof(Guid):
                    dbType = DbType.String;
                    return ((Guid)value).ToString("D");
                case Type _ when underlyingType == typeof(DateTimeOffset):
                    dbType = DbType.DateTime2;
                    return ((DateTimeOffset)value).UtcDateTime;
                case Type _ when underlyingType == typeof(TimeSpan):
                    dbType = DbType.String;
                    return ((TimeSpan)value).ToString("c", CultureInfo.InvariantCulture);
                default:
                    dbType = DbType.String;
                    if (underlyingType.IsClass || underlyingType.IsArray || (underlyingType.IsValueType && underlyingType.Namespace != nameof(System)))
                        return JsonConvert.SerializeObject(value, Formatting.None);

                    return Convert.ToString(value, CultureInfo.InvariantCulture);
            }
        }

        // Snowflake query results can surface CLR values that do not map cleanly to
        // the downstream Oracle-oriented payload expected by the rest of this utility.
        // This method normalizes each field value into a more predictable shape by
        // flattening provider-specific / semi-structured values to text where needed
        // and by coercing selected CLR primitives into representations that are safer
        // for later JSON serialization and Oracle input binding.
        private static object GetSfValue(IDataRecord reader, int ordinal)
        {
            string GetSnowflakeTypeName(IDataRecord rd, int ordin)
            {
                try
                {
                    return rd.GetDataTypeName(ordin);
                }
                catch
                {
                    return null;
                }
            }

            bool IsSemiStructuredSnowflakeType(string sfTypeName)
            {
                return string.Equals(sfTypeName, "ARRAY", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(sfTypeName, "OBJECT", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(sfTypeName, "VARIANT", StringComparison.OrdinalIgnoreCase);
            }

            bool IsSnowflakeSpecificClrType(object val)
            {
                Type valueType = val.GetType();
                return valueType.Namespace != null
                    && valueType.Namespace.StartsWith("Snowflake", StringComparison.Ordinal);
            }

            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            if (ordinal < 0 || ordinal >= reader.FieldCount)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            if (reader.IsDBNull(ordinal))
                return null;

            object value = reader.GetValue(ordinal);

            switch (value)
            {
                case null:
                case DBNull _:
                    return null;
                case bool boolValue:
                    return boolValue ? "Y" : "N";
                case DateTimeOffset dateTimeOffsetValue:
                    return dateTimeOffsetValue.LocalDateTime;
                case TimeSpan timeSpanValue:
                    return timeSpanValue.ToString("c", CultureInfo.InvariantCulture);
                case char charValue:
                    return charValue.ToString();
                case char[] charArrayValue:
                    return new string(charArrayValue);
                case Guid guidValue:
                    return guidValue.ToString("D");
                case float floatValue:
                    if (float.IsNaN(floatValue) || float.IsInfinity(floatValue))
                        return null;
                    else
                        return floatValue;
                case double doubleValue:
                    if (double.IsNaN(doubleValue) || double.IsInfinity(doubleValue))
                        return null;
                    else
                        return doubleValue;
                case sbyte sbyteValue:
                    return (short)sbyteValue;
                case byte byteValue:
                    return (short)byteValue;
                case ushort ushortValue:
                    if (ushortValue <= short.MaxValue)
                        return (short)ushortValue;
                    else
                        return (int)ushortValue;
                case uint uintValue:
                    if (uintValue <= int.MaxValue)
                        return (int)uintValue;
                    else
                        return (long)uintValue;
                case ulong ulongValue:
                    if (ulongValue <= long.MaxValue)
                        return (long)ulongValue;
                    else
                        return (decimal)ulongValue;
                default:
                    string snowflakeTypeName = GetSnowflakeTypeName(reader, ordinal);
                    if (IsSemiStructuredSnowflakeType(snowflakeTypeName) || IsSnowflakeSpecificClrType(value))
                        return Convert.ToString(value, CultureInfo.InvariantCulture);
                    else
                        return value;
            }
        }

        public static IDictionary<string, object> Execute(string configServer, string configJson, string sfQuery, IDictionary<string, object> bindParams, int timeoutSeconds,
            string mapJson = "{}", string mergeJson = "{}")
        {
            SourceConfig sourceConf = Transformer.DeserializeJson<SourceConfig>(configJson);
            string connectionString = SnowflakeConfig.GetConnectionString(configServer, sourceConf.TplKey, sourceConf.OvrParams, false);
            var mapDict = Transformer.ConvertJsonToDict<string>(mapJson, true);
            var mergeDict = Transformer.ConvertJsonToDict<object>(mergeJson, false);

            for (int retries = 0; retries < 2; retries++)
            {
                try
                {
                    using (var client = new SnowflakeClient(connectionString))
                    {
                        OrderedDictionary queryResult = client.Execute(sfQuery: sfQuery, bindParams: bindParams, columnMapping: mapDict, timeoutSeconds: timeoutSeconds);
                        Dictionary<string, object> finalResult = queryResult.OfType<DictionaryEntry>()
                            .ToDictionary(entry => (string)entry.Key, entry => (object)entry.Value, StringComparer.OrdinalIgnoreCase);

                        foreach (var kv in mergeDict)
                        {
                            if (!string.IsNullOrWhiteSpace(kv.Key))
                            {
                                finalResult[kv.Key] = kv.Value;
                            }
                        }

                        return finalResult.EncapsulateIntoJsonParamIfRequired(mapDict);
                    }
                }
                catch (SnowflakeConnException ex)
                {
                    if (retries == 0)
                    {
                        SnowflakeDbConnectionPool.ClearAllPools();
                        Thread.Sleep(SnowflakeConfig.RetryDelay);   // Wait for 10 second before retrying
                        connectionString = SnowflakeConfig.GetConnectionString(configServer, sourceConf.TplKey, sourceConf.OvrParams, true);
                    }
                    else
                    {
                        ExceptionDispatchInfo.Capture(ex.InnerException).Throw();
                    }
                }
            }

            throw new RetryException("Failed to execute Snowflake query after retrying with refreshed connection string.");
        }

        public static IDictionary<string, object> Execute(string configServer, string configJson, string sfQuery, string bindParams, int timeoutSeconds, string mapJson, string mergeJson)
        {
            var namedParamsDict = Transformer.ConvertJsonToDict<object>(bindParams, true);
            return Execute(configServer, configJson, sfQuery, namedParamsDict, timeoutSeconds, mapJson, mergeJson);
        }

        public static bool IsSnowflake(string literalServerOrConfigJson)
        {
            string trimmedSource = literalServerOrConfigJson?.Trim();
            if (string.IsNullOrEmpty(trimmedSource))
                return false;

            return (trimmedSource[0] == '{' && trimmedSource[trimmedSource.Length - 1] == '}');
        }
    }
}
