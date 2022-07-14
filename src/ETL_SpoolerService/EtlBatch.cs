// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using DataBooster.PsInvokeDb.DataAccess;

namespace DataBooster.Simple_ETL_Spooler
{
    public class EtlBatch
    {
        private readonly Poller _edbPoller;

        private readonly int _batchID;
        private readonly IList<IList<EtlTask>> _tasks;
        private string _lastStepPlan;

        public int BatchID => _batchID;

        public EtlBatch(Poller edbPoller, int batchID)
        {
            _edbPoller = edbPoller;

            _batchID = batchID;
            _tasks = new List<IList<EtlTask>>();
            _lastStepPlan = string.Empty;
        }

        public void AddTask(string stepPlan, int taskID, string extractType, string extractSource, string extractCommand, string extractParams, int extractTimeoutSeconds,
            int extractResultSet, string namingConvention, string loadType, string fieldMapping, string mergeParams, string loadDestination, string loadCommand, int loadTimeoutSeconds)
        {
            if (stepPlan != _lastStepPlan)
            {
                _tasks.Add(new List<EtlTask>());
                _lastStepPlan = stepPlan;
            }

            _tasks[_tasks.Count - 1].Add(new EtlTask(taskID, extractType, extractSource, extractCommand, extractParams, extractTimeoutSeconds,
                extractResultSet, namingConvention, loadType, fieldMapping, mergeParams, loadDestination, loadCommand, loadTimeoutSeconds));
        }

        public Task Run()
        {
            bool RunParallelTasks(IList<EtlTask> pTasks)
            {
                if (pTasks == null || pTasks.Count == 0)
                    return true;

                if (pTasks.Count == 1)
                {
                    EtlTask et = pTasks[0];
                    et.Run();
                    _edbPoller.EndTask(et.TaskID, et.RuntimeError);
                    return string.IsNullOrEmpty(et.RuntimeError);
                }
                else
                {
                    Parallel.ForEach(pTasks, t => { t.Run(); _edbPoller.EndTask(t.TaskID, t.RuntimeError); });
                    return pTasks.All(t => string.IsNullOrEmpty(t.RuntimeError));
                }
            }

            return Task.Run(() =>
            {
                bool success;

                for (int i = 0; i < _tasks.Count; i++)
                {
                    success = RunParallelTasks(_tasks[i]);

                    if (!success && ConfigHelper.StopSerialOnError)
                        break;
                }

                _edbPoller.End_Batch(BatchID);
            });
        }
    }
}
