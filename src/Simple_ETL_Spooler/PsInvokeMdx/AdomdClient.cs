// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Data;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using Microsoft.AnalysisServices.AdomdClient;
using Newtonsoft.Json;
using System;

namespace DataBooster.PsInvokeMdx
{
    public class AdomdClient : IDisposable
    {
        private static readonly Regex _shortenColumnNameReg = new Regex(@"(?<sb>\[)?(?<col>(?(sb)([^\]]|\]\])*|\w*))(?(sb)\]|\b)\s*(\.\s*\[MEMBER_CAPTION\]\s*$|\.\s*MEMBER_CAPTION\s*$|$)");
        private AdomdConnection _connection;

        public AdomdClient(string connectionString)
        {
            _connection = new AdomdConnection(connectionString);
        }

        public void Enter()
        {
            switch (_connection.State)
            {
                case ConnectionState.Closed:
                    _connection.Open();
                    break;
                case ConnectionState.Broken:
                    _connection.Close();
                    _connection.Open();
                    break;
            }
        }

        public void Exit()
        {
            if (_connection.State != ConnectionState.Closed)
                _connection.Close();
        }

        private static string ShortenColumnName(string longName, bool shorten)
        {
            if (shorten)
            {
                Match match = _shortenColumnNameReg.Match(longName);

                if (match.Success)
                    return match.Groups["col"].Value.Trim().Replace("]]", "]");
            }

            return longName.Trim();
        }

        public StoredProcedureResponse Execute(string mdxQuery, bool shorten, int timeoutSeconds = 0)
        {
            StoredProcedureResponse spResponse = new StoredProcedureResponse();

            using (var cmd = new AdomdCommand(mdxQuery, _connection))
            {
                if (timeoutSeconds > 0)
                    cmd.CommandTimeout = timeoutSeconds;

                using (var reader = cmd.ExecuteReader())
                {
                    do
                    {
                        var resultSet = new List<IDictionary<string, object>>();
                        string[] columnNames = new string[reader.FieldCount];

                        if (columnNames.Length > 0)
                        {
                            for (int i = 0; i < columnNames.Length; i++)
                                columnNames[i] = ShortenColumnName(reader.GetName(i), shorten);

                            while (reader.Read())
                            {
                                var record = new Dictionary<string, object>(columnNames.Length);

                                for (int i = 0; i < columnNames.Length; i++)
                                {
                                    record[columnNames[i]] = reader.GetValue(i);
                                }
                                resultSet.Add(record);
                            }

                            spResponse.ResultSets.Add(resultSet);
                        }

                    } while (reader.NextResult());
                }
            }

            return spResponse;
        }

        public string ExecuteAsJson(string mdxQuery, bool shorten, bool indent)
        {
            Enter();

            StoredProcedureResponse mdxResult = Execute(mdxQuery, shorten, 0);
            string jsonResult = JsonConvert.SerializeObject(mdxResult, indent ? Formatting.Indented : Formatting.None);

            Exit();
            return jsonResult;
        }

        public void Dispose()
        {
            _connection.Dispose();
        }
    }
}
