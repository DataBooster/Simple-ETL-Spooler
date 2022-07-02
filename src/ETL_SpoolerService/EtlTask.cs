// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Data;
using System.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.Http.Headers;
using DataBooster.PsInvokeDb;
using DataBooster.PsInvokeMdx;
using DataBooster.PsConvertToOracleInput;
using DataBooster.DbWebApi.Client;
using DataBooster.PsInvokeDb.DataAccess;

namespace DataBooster.Simple_ETL_Spooler
{
    public class EtlTask
    {
        private readonly int _taskID;
        private readonly string _extractType;
        private readonly string _extractSource;
        private readonly string _extractCommand;
        private readonly string _extractParams;
        private readonly int _extractTimeoutSeconds;
        private readonly int _extractResultSet;
        private readonly string _namingConvention;
        private readonly string _loadType;
        private readonly string _fieldMapping;
        private readonly string _mergeParams;
        private readonly string _loadDestination;
        private readonly string _loadCommand;
        private readonly int _loadTimeoutSeconds;
        private string _runtimeError;

        public int TaskID => _taskID;
        public string RuntimeError => _runtimeError;
        public bool IsCompleted => _runtimeError != null;

        public EtlTask(int taskID, string extractType, string extractSource, string extractCommand, string extractParams, int extractTimeoutSeconds, int extractResultSet,
            string namingConvention, string loadType, string fieldMapping, string mergeParams, string loadDestination, string loadCommand, int loadTimeoutSeconds)
        {
            _taskID = taskID;
            _extractType = extractType.ToUpper();
            _extractSource = extractSource;
            _extractCommand = extractCommand;
            _extractParams = extractParams;
            _extractTimeoutSeconds = extractTimeoutSeconds;
            _extractResultSet = extractResultSet;
            _namingConvention = namingConvention;
            _loadType = loadType.ToUpper();
            _fieldMapping = fieldMapping;
            _mergeParams = mergeParams;
            _loadDestination = loadDestination;
            _loadCommand = loadCommand;
            _loadTimeoutSeconds = loadTimeoutSeconds;
            _runtimeError = null;

            if (_extractType != "SP" && _extractType != "SQL" && _extractType != "MDX" && _extractType != "REST")
                AppendError($"'{_extractType}' ExtractType is not supported!");
            if (_loadType != "SP")
                AppendError($"'{_loadType}' LoadType is not supported!");
        }

        private void AppendError(string errorMsg)
        {
            if (errorMsg == null)
                return;
            if (_runtimeError == null)
                _runtimeError = errorMsg;
            else
                _runtimeError += "\n" + errorMsg;
        }

        public void Run()
        {
            if (IsCompleted)
                return;

            try
            {
                dynamic extractResult;

                switch (_extractType)
                {
                    case "SP":
                        extractResult = Poller.Execute(dbServer: _extractSource, commandText: _extractCommand, commandType: CommandType.StoredProcedure,
                            parameters: InvokeDbCmdlet.ToDict(_extractParams, "extractParams", _extractCommand), namingConvention: _namingConvention, timeoutSeconds: _extractTimeoutSeconds);
                        break;
                    case "SQL":
                        extractResult = Poller.Execute(dbServer: _extractSource, commandText: _extractCommand, commandType: CommandType.Text,
                            parameters: null, namingConvention: _namingConvention, timeoutSeconds: _extractTimeoutSeconds);
                        break;
                    case "MDX":
                        using (AdomdClient mdx = new AdomdClient(_extractSource))
                        {
                            mdx.Enter();
                            extractResult = mdx.Execute(_extractCommand, true, _extractTimeoutSeconds);
                            mdx.Exit();
                        }
                        break;
                    case "REST":
                        extractResult = InvokeRest(_extractSource, _extractCommand, _extractParams, _extractTimeoutSeconds);
                        break;
                    default:
                        extractResult = null;
                        break;
                }

                if (extractResult != null && _loadType == "SP")
                {
                    IDictionary<string, object> loadInput;

                    if (_extractType == "REST")
                    {
                        string jsonInputParamName = Transformer.ConvertJsonToDict<string>(_fieldMapping, false).Values.FirstOrDefault(v => !string.IsNullOrWhiteSpace(v))?.Trim();

                        loadInput = Transformer.ConvertJsonToDict<object>(_mergeParams, true);

                        if (!string.IsNullOrEmpty(jsonInputParamName) && !loadInput.ContainsKey(jsonInputParamName))
                            loadInput.Add(jsonInputParamName, extractResult);
                    }
                    else
                    {
                        loadInput = Transformer.ConvertToOracleInput(extractResult, _extractResultSet, _fieldMapping, _mergeParams, true);
                    }

                    var loadResult = Poller.Execute(dbServer: _loadDestination, commandText: _loadCommand, commandType: CommandType.StoredProcedure,
                            parameters: loadInput, namingConvention: _namingConvention, timeoutSeconds: _loadTimeoutSeconds);
                }

                _runtimeError = string.Empty;
            }
            catch (Exception err)
            {
                AppendError($"{err.GetType().Name}: {err.Message}");
                if (err.InnerException != null)
                    AppendError($"\t{err.InnerException.GetType().Name}: {err.InnerException.Message}");
            }
        }

        private static string InvokeRest(string url, string payload, string controlParams, int timeoutSeconds = 0)
        {
            string TryTakeOut(IDictionary<string, string> dict, string key)
            {
                if (dict.TryGetValue(key, out string value))
                {
                    dict.Remove(key);
                    return value.Trim();
                }
                else
                    return null;
            }

            const string content_TypeLabel = "Content-Type";
            const string http_MethodLabel = "Http-Method";
            const string httpMethodLabel = "HttpMethod";
            const string acceptLabel = "Accept";

            IDictionary<string, string> controlDict = Transformer.ConvertJsonToDict<string>(controlParams, true);
            string mediaType = TryTakeOut(controlDict, content_TypeLabel);
            string httpMethod = TryTakeOut(controlDict, http_MethodLabel);
            if (string.IsNullOrEmpty(httpMethod))
                httpMethod = TryTakeOut(controlDict, httpMethodLabel);
            string accept = TryTakeOut(controlDict, acceptLabel);

            using (DbWebApiClient restClient = new DbWebApiClient())
            {
                if (timeoutSeconds > 0)
                    restClient.HttpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

                if (!string.IsNullOrWhiteSpace(httpMethod))
                    restClient.HttpMethod = new HttpMethod(httpMethod.Trim().ToUpper());

                if (!string.IsNullOrEmpty(accept))
                    restClient.AcceptMediaTypes.Add(new MediaTypeWithQualityHeaderValue(accept));

                if (string.IsNullOrWhiteSpace(payload))
                    payload = string.Empty;

                return restClient.ExecAsString(url, payload, null, mediaType, headers => { foreach (var kvp in controlDict) { headers.Add(kvp.Key, kvp.Value); } });
            }
        }
    }
}
