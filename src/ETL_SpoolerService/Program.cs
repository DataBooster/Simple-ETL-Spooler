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
            ServiceBase[] ServicesToRun;
            ServicesToRun = new ServiceBase[]
            {
                new WindowsService()
            };
            ServiceBase.Run(ServicesToRun);
        }
    }
}
