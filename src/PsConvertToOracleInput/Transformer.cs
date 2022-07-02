// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Management.Automation;
using System.ComponentModel;
//using System.Text.Json;
using Newtonsoft.Json;

namespace DataBooster.PsConvertToOracleInput
{
    public static class Transformer
    {
        //static readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions() { PropertyNameCaseInsensitive = true, AllowTrailingCommas = true, ReadCommentHandling = JsonCommentHandling.Skip };

        internal static string MustJson(string mayJson)
        {
            return string.IsNullOrWhiteSpace(mayJson) ? "{}" : mayJson;
        }

        internal static IDictionary<string, T> ToDict<T>(object inputObject, string argName)
        {
            if (inputObject is PSObject psObject)
                inputObject = psObject.BaseObject;

            switch (inputObject)
            {
                case IDictionary<string, T> dict:
                    return dict;
                case IDictionary hashTable:
                    return hashTable.Cast<DictionaryEntry>().ToDictionary(x => (string)x.Key, x => (T)x.Value);
                case string json:
                    return JsonConvert.DeserializeObject<IDictionary<string, T>>(MustJson(json));
                default:
                    if (inputObject == null)
                        return null;

                    Type inType = inputObject.GetType();

                    if (inType.IsPrimitive || inType.IsEnum || inType.Equals(typeof(decimal)))
                    {
                        throw new ArgumentException("Only accepts a dictionary, hashtable, JSON string or anonymous object.", argName);
                    }
                    else
                    {
                        PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(inputObject);
                        var dict = new Dictionary<string, T>(properties.Count, StringComparer.OrdinalIgnoreCase);

                        foreach (PropertyDescriptor prop in properties)
                            dict.Add(prop.Name, (T)prop.GetValue(inputObject));

                        return dict;
                    }
            }
        }

        public static IDictionary<string, object> AsOracleInput(this StoredProcedureResponse spResponse, IDictionary<string, string> columnMapping, int resultSet = 0)
        {
            string MapColumnToParam(string columnName)
            {
                if (columnMapping != null && columnMapping.TryGetValue(columnName, out string toName))
                    return toName;
                else
                    return columnName;
            }

            var toDict = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
            string paramName;

            if (spResponse.ReturnValue != null)
            {
                paramName = MapColumnToParam("ReturnValue");
                if (!string.IsNullOrWhiteSpace(paramName))
                    toDict[paramName] = spResponse.ReturnValue;
            }

            if (spResponse.OutputParameters != null)
            {
                foreach (var kv in spResponse.OutputParameters)
                {
                    paramName = MapColumnToParam(kv.Key);
                    if (!string.IsNullOrWhiteSpace(paramName))
                        toDict[paramName] = kv.Value;
                }
            }

            if (spResponse.ResultSets.Length > resultSet)
            {
                var data = spResponse.ResultSets[resultSet];

                if (data.Length > 0)
                {
                    foreach (var kv in data[0])
                    {
                        paramName = MapColumnToParam(kv.Key);
                        if (!string.IsNullOrWhiteSpace(paramName))
                            toDict[paramName] = new object[data.Length];
                    }

                    for (int i = 0; i < data.Length; i++)
                    {
                        foreach (var kv in data[i])
                        {
                            paramName = MapColumnToParam(kv.Key);
                            if (!string.IsNullOrWhiteSpace(paramName))
                            {
                                var p = toDict[paramName] as object[];
                                if (p != null)
                                    p[i] = kv.Value;
                            }
                        }
                    }
                }
            }

            return toDict;
        }

        public static IDictionary<string, object> Merge(this IDictionary<string, object> baseDict, IDictionary<string, object> mergeDict, bool overrideMerge)
        {
            foreach (var kv in mergeDict)
            {
                if ((overrideMerge && !string.IsNullOrWhiteSpace(kv.Key)) || !baseDict.ContainsKey(kv.Key))
                {
                    baseDict[kv.Key] = kv.Value;
                }
            }

            return baseDict;
        }

        public static IDictionary<string, T> AsCaseInsensitive<T>(this IDictionary<string, T> mapJson)
        {
            if (mapJson is null)
                return new Dictionary<string, T>(0, StringComparer.OrdinalIgnoreCase);
            else
                return new Dictionary<string, T>(mapJson, StringComparer.OrdinalIgnoreCase);
        }

        public static string ToOracleInputJson(string inputJson, int resultSet, string mapJson, string mergeJson, bool overrideMerge, bool indent = false)
        {
            //var inputResult = JsonSerializer.Deserialize<StoredProcedureResponse>(MustJson(inputJson), _jsonSerializerOptions) ?? new StoredProcedureResponse();
            //var mapDict = JsonSerializer.Deserialize<IDictionary<string, string?>>(MustJson(mapJson), _jsonSerializerOptions).AsCaseInsensitive();
            //var mergeDict = JsonSerializer.Deserialize<IDictionary<string, object?>>(MustJson(mergeJson), _jsonSerializerOptions) ?? new Dictionary<string, object?>(0);
            //var transformed = inputResult.AsOracleInput(mapDict, resultSet).Merge(mergeDict, overrideMerge);

            //return JsonSerializer.Serialize(transformed, new JsonSerializerOptions { WriteIndented = indent });

            var inputResult = JsonConvert.DeserializeObject<StoredProcedureResponse>(MustJson(inputJson)) ?? new StoredProcedureResponse();
            var transformed = ConvertToOracleInput(inputResult, resultSet, mapJson, mergeJson, overrideMerge);

            return JsonConvert.SerializeObject(transformed, indent ? Formatting.Indented : Formatting.None);
        }

        public static IDictionary<string, object> ConvertToOracleInput(object sourceResponse, int resultSet, string mapJson, string mergeJson, bool overrideMerge = true)
        {
            StoredProcedureResponse spResponse = StoredProcedureResponse.FromObject(sourceResponse);
            var mapDict = ConvertJsonToDict<string>(mapJson, true);
            var mergeDict = JsonConvert.DeserializeObject<IDictionary<string, object>>(MustJson(mergeJson)) ?? new Dictionary<string, object>(0);
            var transformed = spResponse.AsOracleInput(mapDict, resultSet).Merge(mergeDict, overrideMerge);

            return transformed;
        }

        public static IDictionary<string, T> ConvertJsonToDict<T>(string dictJson, bool ignoreCase = true)
        {
            var dict = JsonConvert.DeserializeObject<IDictionary<string, T>>(MustJson(dictJson));
            return ignoreCase ? dict.AsCaseInsensitive() : dict;
        }
    }
}
