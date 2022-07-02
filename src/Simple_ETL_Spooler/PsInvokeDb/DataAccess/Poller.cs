// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Linq;
using System.Collections.Generic;
using DbParallel.DataAccess;
using System.Data;

namespace DataBooster.PsInvokeDb.DataAccess
{
    public class Poller : IDisposable
    {
        private readonly DalCenter _edbClient;
        private readonly string _queueServer;
        private readonly string _pollingPackage;

        public Poller(string queueServer, string pollingPackage)
        {
            if (string.IsNullOrWhiteSpace(queueServer))
                throw new ArgumentNullException(nameof(queueServer));
            if (string.IsNullOrWhiteSpace(pollingPackage))
                throw new ArgumentNullException(nameof(pollingPackage));

            _queueServer = queueServer;
            _pollingPackage = pollingPackage;

            _edbClient = CreatePollingConnection();
        }

        private DalCenter CreatePollingConnection()
        {
            var pollingConnection = DalCenter.CreateClient(_queueServer);
            pollingConnection.DynamicPropertyNamingConvention = PropertyNamingConvention.None;
            return pollingConnection;
        }

        public IEnumerable<(
            int BatchID,
            string StepPlan,
            int TaskID,
            string ExtractType,
            string ExtractSource,
            string ExtractCommand,
            string ExtractParams,
            int ExtractTimeoutSeconds,
            int ExtractResultSet,
            string NamingConvention,
            string LoadType,
            string FieldMapping,
            string MergeParams,
            string LoadDestination,
            string LoadCommand,
            int LoadTimeoutSeconds)>
            PollTaskQueue()
        {
            var resp = _edbClient.ExecuteProcedure(CompletePollingSpName("POLL_TASK_QUEUE"));
            return resp.ResultSets[0].Select(row => (
                BatchID: row.Property<int>("BATCH_ID"),
                StepPlan: row.Property<string>("STEP_PLAN"),
                TaskID: row.Property<int>("TASK_ID"),
                ExtractType: row.Property<string>("EXTRACT_TYPE"),
                ExtractSource: row.Property<string>("EXTRACT_SOURCE"),
                ExtractCommand: row.Property<string>("EXTRACT_COMMAND"),
                ExtractParams: row.Property<string>("EXTRACT_PARAMS"),
                ExtractTimeoutSeconds: row.Property<int>("EXTRACT_TIMEOUT_SEC"),
                ExtractResultSet: row.Property<int>("RESULT_SET"),
                NamingConvention: row.Property<string>("NAMING_CONVENTION"),
                LoadType: row.Property<string>("LOAD_TYPE"),
                FieldMapping: row.Property<string>("FIELD_MAPPING"),
                MergeParams: row.Property<string>("MERGE_PARAMS"),
                LoadDestination: row.Property<string>("LOAD_DESTINATION"),
                LoadCommand: row.Property<string>("LOAD_COMMAND"),
                LoadTimeoutSeconds: row.Property<int>("LOAD_TIMEOUT_SEC")));
        }

        public void EndTask(int taskId, string errorMsg)
        {
            using (var dal = CreatePollingConnection())
            {
                dal.ExecuteProcedure(CompletePollingSpName("END_TASK"), new { inTask_ID = taskId, inRuntime_Error = errorMsg });
            }
        }

        public void End_Batch(int batchId)
        {
            using (var dal = CreatePollingConnection())
            {
                dal.ExecuteProcedure(CompletePollingSpName("END_BATCH"), new { inBatch_ID = batchId });
            }
        }

        private string CompletePollingSpName(string sp)
        {
            if (string.IsNullOrEmpty(sp))
                throw new ArgumentNullException("sp");
            else
                return _pollingPackage + sp;
        }

        public static dynamic Execute(string dbServer, string commandText, CommandType commandType, IDictionary<string, object> parameters, string namingConvention, int timeoutSeconds)
        {
            return DalCenter.Execute(dbServer, commandText, commandType, parameters, namingConvention, timeoutSeconds);
        }

        public void Reconnect()
        {
            _edbClient.Reconnect();
        }

        public void Dispose()
        {
            _edbClient.Dispose();
        }
    }
}
