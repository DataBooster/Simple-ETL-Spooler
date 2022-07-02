// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Data;
using System.Collections.Generic;
using Newtonsoft.Json;

namespace DataBooster.PsInvokeDb.DataAccess
{
    public static class DbCmd
    {
        public static object RunSp(string dbServer, string storedProcedure, IDictionary<string, object> parameters = null, string namingConvention = "N")
        {
            return DalCenter.Execute(dbServer, storedProcedure, CommandType.StoredProcedure, parameters, namingConvention);
        }

        public static string RunSpAsJson(string dbServer, string storedProcedure, string jsonParameters = null, string namingConvention = "N", bool indent = false)
        {
            IDictionary<string, object> parameters = string.IsNullOrWhiteSpace(jsonParameters) ? null : JsonConvert.DeserializeObject<IDictionary<string, object>>(jsonParameters);
            var dbResult = RunSp(dbServer, storedProcedure, parameters, namingConvention);
            return JsonConvert.SerializeObject(dbResult, indent ? Formatting.Indented : Formatting.None);
        }

        public static object RunQuery(string dbServer, string dynamicQuery, string namingConvention = "N")
        {
            return DalCenter.Execute(dbServer, dynamicQuery, CommandType.Text, null, namingConvention);
        }

        public static string RunQueryAsJson(string dbServer, string dynamicQuery, string namingConvention = "N", bool indent = false)
        {
            var dbResult = DbCmd.RunQuery(dbServer, dynamicQuery, namingConvention);
            return JsonConvert.SerializeObject(dbResult, indent ? Formatting.Indented : Formatting.None);
        }
    }
}
