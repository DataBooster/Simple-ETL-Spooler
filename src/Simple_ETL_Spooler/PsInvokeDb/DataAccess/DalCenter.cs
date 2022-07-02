// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;
using DbParallel.DataAccess;

namespace DataBooster.PsInvokeDb.DataAccess
{
    public partial class DalCenter
    {
        private DalCenter(DbProviderFactory dbProviderFactory, string connectionString)
            : base(dbProviderFactory, connectionString)
        {
            OnInit();
        }

        internal static DalCenter CreateClient(string dbServer)
        {
            var (dbProviderFactory, connectionString) = Customizer.GetDbProvider(dbServer);

            return new DalCenter(dbProviderFactory, connectionString);
        }

        internal StoredProcedureResponse ExecuteQuery(string queryText, int timeoutSeconds)
        {
            var req = new StoredProcedureRequest(queryText) { CommandType = CommandType.Text };

            if (timeoutSeconds > 0)
                req.CommandTimeout = timeoutSeconds;

            return AccessChannel.ExecuteStoredProcedure(req);
        }

        internal StoredProcedureResponse ExecuteProcedure(string commandText, IDictionary<string, object> parameters, int timeoutSeconds)
        {
            var req = new StoredProcedureRequest(commandText, parameters) { CommandType = CommandType.StoredProcedure };
            if (timeoutSeconds > 0)
                req.CommandTimeout = timeoutSeconds;
            return AccessChannel.ExecuteStoredProcedure(req);
        }

        internal static StoredProcedureResponse Execute(string dbServer, string commandText, CommandType commandType = CommandType.StoredProcedure,
            IDictionary<string, object> parameters = null, string namingConvention = "N", int timeoutSeconds = 0)
        {
            if (string.IsNullOrWhiteSpace(dbServer))
                throw new ArgumentNullException(nameof(dbServer));
            if (string.IsNullOrWhiteSpace(commandText))
                throw new ArgumentNullException(nameof(commandText));
            if (namingConvention == null)
                namingConvention = "N";

            dbServer = dbServer.Trim().ToUpper();

            using (var dal = DalCenter.CreateClient(dbServer))
            {
                switch (namingConvention.Substring(0, 1).ToUpper())
                {
                    case "P":
                        dal.DynamicPropertyNamingConvention = PropertyNamingConvention.PascalCase;
                        break;
                    case "C":
                        dal.DynamicPropertyNamingConvention = PropertyNamingConvention.CamelCase;
                        break;
                    default:
                        dal.DynamicPropertyNamingConvention = PropertyNamingConvention.None;
                        break;
                }

                return (commandType == CommandType.Text) ? dal.ExecuteQuery(commandText, timeoutSeconds) : dal.ExecuteProcedure(commandText, parameters, timeoutSeconds);
            }
        }

        internal void Reconnect()
        {
            var state = AccessChannel.Connection.State;
            if (!state.HasFlag(ConnectionState.Open) && !state.HasFlag(ConnectionState.Connecting))
            {
                AccessChannel.Connection.Close();
            }
            AccessChannel.Connection.Open();
        }
    }
}
