// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Management.Automation;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace DataBooster.PsConvertToOracleInput
{
    public static class Transformer
    {
        //static readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions() { PropertyNameCaseInsensitive = true, AllowTrailingCommas = true, ReadCommentHandling = JsonCommentHandling.Skip };
        private static readonly Regex _regexSingleJsonParamName = new Regex(@"^\w+$");

        private static string MustJson(string mayJson)
        {
            return string.IsNullOrWhiteSpace(mayJson) ? "{}" : mayJson;
        }

        public static T DeserializeJson<T>(string strJson)
        {
            return JsonConvert.DeserializeObject<T>(MustJson(strJson));
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
                    return DeserializeJson<IDictionary<string, T>>(json) ?? new Dictionary<string, T>(0);
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

            if (spResponse == null)
                throw new ArgumentNullException(nameof(spResponse));

            if (resultSet < 0)
                throw new ArgumentOutOfRangeException(nameof(resultSet));

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
            if (mergeDict != null)
            {
                foreach (var kv in mergeDict)
                {
                    if ((overrideMerge && !string.IsNullOrWhiteSpace(kv.Key)) || !baseDict.ContainsKey(kv.Key))
                    {
                        baseDict[kv.Key] = kv.Value;
                    }
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
            var inputResult = DeserializeJson<StoredProcedureResponse>(inputJson) ?? new StoredProcedureResponse();
            var transformed = ConvertToOracleInput(inputResult, resultSet, mapJson, mergeJson, overrideMerge);

            return JsonConvert.SerializeObject(transformed, indent ? Formatting.Indented : Formatting.None);
        }

        public static IDictionary<string, object> ConvertToOracleInput(object sourceResponse, int resultSet, string mapJson, string mergeJson, bool overrideMerge = true)
        {
            StoredProcedureResponse spResponse = StoredProcedureResponse.FromObject(sourceResponse);
            var mapDict = ConvertJsonToDict<string>(mapJson, true);
            var mergeDict = DeserializeJson<IDictionary<string, object>>(mergeJson) ?? new Dictionary<string, object>(0);
            var transformed = spResponse.AsOracleInput(mapDict, resultSet).Merge(mergeDict, overrideMerge);

            return transformed.EncapsulateIntoJsonParamIfRequired(mapDict);
        }

        public static IDictionary<string, T> ConvertJsonToDict<T>(string dictJson, bool ignoreCase = true)
        {
            var dict = DeserializeJson<IDictionary<string, T>>(dictJson);
            return ignoreCase ? dict.AsCaseInsensitive() : (dict ?? new Dictionary<string, T>(0));
        }

        public static IDictionary<string, object> EncapsulateIntoJsonParamIfRequired(this IDictionary<string, object> resultDict, IDictionary<string, string> mapDict)
        {
            if (mapDict != null && mapDict.TryGetValue("*", out string toJsonParamName) && _regexSingleJsonParamName.IsMatch(toJsonParamName))
            {
                string jsonText = JsonConvert.SerializeObject(resultDict, Formatting.None);
                var encapsulatedDict = new Dictionary<string, object>(1, StringComparer.OrdinalIgnoreCase) { { toJsonParamName, jsonText } };
                return encapsulatedDict;
            }
            else
                return resultDict;
        }
    }
}
