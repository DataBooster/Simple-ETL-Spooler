using System.ServiceProcess;

namespace DataBooster.Simple_ETL_Spooler
{
    public partial class WindowsService : ServiceBase
    {
        private QueuePolling _polling;

        public WindowsService()
        {
            InitializeComponent();
            _polling = new QueuePolling();
        }

        protected override void OnStart(string[] args)
        {
            _polling.Start(EventLog);
        }

        protected override void OnStop()
        {
            _polling.Stop();
        }
    }
}
