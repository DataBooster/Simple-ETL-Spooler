// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Timers;
using System.Diagnostics;
using DataBooster.PsInvokeDb.DataAccess;

namespace DataBooster.Simple_ETL_Spooler
{
    public class QueuePolling
    {
        private Timer _pollingTimer;
        private Poller _edbPoller;
        private EventLog _eventLog;
        private EtlBatch _currentBatch;
        private bool _busy;

        public QueuePolling()
        {
            _pollingTimer = new Timer(ConfigHelper.IntervalMilliseconds);
            _pollingTimer.Elapsed += this.OnTimedEvent;
            _busy = false;
            _edbPoller = null;
            _eventLog = null;
        }

        public void Start(EventLog eventLog)
        {
            _eventLog = eventLog;
            _edbPoller = new Poller(ConfigHelper.MainDB, ConfigHelper.MainPackage);
            _pollingTimer.Start();
        }

        public void Stop()
        {
            _pollingTimer.Stop();
            _edbPoller.Dispose();
            _busy = false;
        }

        private void OnTimedEvent(object sender, ElapsedEventArgs args)
        {
            PollTask();
        }

        private void PollTask()
        {
            if (_busy)
                return;

            try
            {
                _busy = true;
                _currentBatch = null;

                foreach (var task in _edbPoller.PollTaskQueue())
                {
                    if (_currentBatch != null && task.BatchID != _currentBatch.BatchID)
                        _currentBatch.Run();

                    if (_currentBatch == null || task.BatchID != _currentBatch.BatchID)
                        _currentBatch = new EtlBatch(_edbPoller, task.BatchID);

                    _currentBatch.AddTask(
                        stepPlan: task.StepPlan,
                        taskID: task.TaskID,
                        extractType: task.ExtractType,
                        extractSource: task.ExtractSource,
                        extractCommand: task.ExtractCommand,
                        extractParams: task.ExtractParams,
                        extractTimeoutSeconds: task.ExtractTimeoutSeconds,
                        extractResultSet: task.ExtractResultSet,
                        namingConvention: task.NamingConvention,
                        loadType: task.LoadType,
                        fieldMapping: task.FieldMapping,
                        mergeParams: task.MergeParams,
                        loadDestination: task.LoadDestination,
                        loadCommand: task.LoadCommand,
                        loadTimeoutSeconds: task.LoadTimeoutSeconds);
                }

                _currentBatch?.Run();
            }
            catch (InvalidOperationException dbErr) when (dbErr.Source == "Oracle.DataAccess")
            {
                TryReconnectEdb(dbErr.Message);
            }
            catch (Exception oraErr) when (oraErr.GetType().Name == "OracleException")
            {
                TryReconnectEdb(oraErr.Message);
            }
            catch (Exception err)
            {
                string errMsg = err.Message;
                if (err.InnerException != null && !string.IsNullOrEmpty(err.InnerException.Message))
                    errMsg += "\n" + err.InnerException.Message;

                if (!string.IsNullOrWhiteSpace(errMsg))
                    LogError(errMsg);
            }
            finally
            {
                _busy = false;
            }
        }

        private void TryReconnectEdb(string reason)
        {
            try
            {
                LogWarning(reason + " - trying to reconnect...");
                _edbPoller.Reconnect();
                LogInfo($"Reconnected to {ConfigHelper.MainDB}");
            }
            catch (Exception err)
            {
                LogError("Reconnect failed: " + err.Message);
            }
        }

        private void LogInfo(string msg)
        {
            _eventLog?.WriteEntry(msg, EventLogEntryType.Information);
        }

        private void LogWarning(string msg)
        {
            _eventLog?.WriteEntry(msg, EventLogEntryType.Warning);
        }
        private void LogError(string msg)
        {
            _eventLog?.WriteEntry(msg, EventLogEntryType.Error);
        }
    }
}
