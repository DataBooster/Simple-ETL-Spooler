using System;
using System.Data;
using System.Collections.Generic;
using DbParallel.DataAccess;

namespace PsInvokeDb.DataAccess
{
    public partial class DalCenter
    {
        private string _queueServer;
        private string _pollingPackage;

        public static DalCenter CreatePollingConnection(string queueServer, string pollingPackage)
        {
            if (string.IsNullOrWhiteSpace(queueServer))
                throw new ArgumentNullException(nameof(queueServer));
            if (string.IsNullOrWhiteSpace(pollingPackage))
                throw new ArgumentNullException(nameof(pollingPackage));

            var dal = CreateClient(queueServer, queueServer.StartsWith("ED", StringComparison.OrdinalIgnoreCase));
            dal.DynamicPropertyNamingConvention = PropertyNamingConvention.None;
            dal._queueServer = queueServer;
            dal._pollingPackage = pollingPackage;
            return dal;
        }

        public IList<BindableDynamicObject> PollTaskQueue()
        {
            var resp = ExecuteProcedure(CompletePollingSpName("POLL_TASK_QUEUE"));
            return resp.ResultSets[0];
        }

        public void EndTask(int taskId, string errorMsg)
        {
            using (var dal = CreatePollingConnection(_queueServer, _pollingPackage))
            {
                dal.ExecuteProcedure(dal.CompletePollingSpName("END_TASK"), new { inTask_ID = taskId, inRuntime_Error = errorMsg });
            }
        }

        public void End_Batch(int batchId)
        {
            using (var dal = CreatePollingConnection(_queueServer, _pollingPackage))
            {
                dal.ExecuteProcedure(dal.CompletePollingSpName("END_BATCH"), new { inBatch_ID = batchId });
            }
        }

        private string CompletePollingSpName(string sp)
        {
            if (string.IsNullOrEmpty(sp))
                throw new ArgumentNullException("sp");
            else
                return _pollingPackage + sp;
        }

        public void Reconnect()
        {
            var state = AccessChannel.Connection.State;
            if (!state.HasFlag(ConnectionState.Open) && !state.HasFlag(ConnectionState.Connecting))
            {
                AccessChannel.Connection.Close();
            }
            AccessChannel.Connection.Open();
        }

        protected override void Dispose(bool disposing)
        {
            try
            {
                base.Dispose(disposing);
            }
            catch
            {
            }
        }
    }
}
