using System;
using System.ServiceProcess;

namespace DataBooster.Simple_ETL_Spooler
{
    static class Program
    {
        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        static void Main()
        {
            if (Environment.UserInteractive)
            {
                WindowsService.DebugInteractive(TimeSpan.FromMinutes(10));
            }
            else
            {
                ServiceBase[] ServicesToRun;
                ServicesToRun = new ServiceBase[] { new WindowsService() };
                ServiceBase.Run(ServicesToRun);
            }
        }
    }
}
