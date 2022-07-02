// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Management.Automation;
using Newtonsoft.Json;

namespace DataBooster.PsInvokeMdx
{
    [Cmdlet("Invoke", "MDX")]
    [OutputType(typeof(StoredProcedureResponse))]
    [Alias("Run-MDX")]
    public class InvokeMdxCmdlet : PSCmdlet
    {
        private AdomdClient _mdxClient;

        [Parameter(Mandatory = true, HelpMessage = "The full connection string used to specify the analytics data source. E.g. \"Provider=MSOLAP;Data Source=http://yourmcdev/AdaptivMemoryCube/;Initial Catalog=YourCube;\"")]
        [Alias("Connection")]
        public string ConnectionString { get; set; }

        [Parameter(Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true, HelpMessage = "The MDX query to be executed.")]
        [Alias("MDX", "Query")]
        public string MdxQuery { get; set; }

        [Parameter(HelpMessage = "Whether to return the raw full column names? By default, only shortened column names are returned.")]
        [Alias("FullColumn")]
        public SwitchParameter FullColumnName { get { return _fullColumnName; } set { _fullColumnName = value; } }
        private bool _fullColumnName = false;

        [Parameter(HelpMessage = "Whether to convert the result object as a JSON-formatted string.")]
        [Alias("json")]
        public SwitchParameter AsJson { get { return _asJson; } set { _asJson = value; } }
        private bool _asJson = false;

        [Parameter(HelpMessage = "Whether to indent the output JSON text.")]
        public SwitchParameter Indent { get { return _indent; } set { _indent = value; } }
        private bool _indent = false;

        protected override void BeginProcessing()
        {
            base.BeginProcessing();
            _mdxClient = new AdomdClient(ConnectionString);
        }

        protected override void ProcessRecord()
        {
            _mdxClient.Enter();

            StoredProcedureResponse dbResult = _mdxClient.Execute(MdxQuery, !FullColumnName);

            if (_asJson)
            {
                WriteObject(JsonConvert.SerializeObject(dbResult, _indent ? Formatting.Indented : Formatting.None));
            }
            else
            {
                WriteObject(dbResult);
            }
        }

        protected override void EndProcessing()
        {
            base.EndProcessing();
            _mdxClient.Exit();
        }
    }
}
