// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Linq;
using System.Collections.Generic;
using System.Management.Automation;
//using System.Text.Json;
using Newtonsoft.Json;

namespace DataBooster.PsConvertToOracleInput
{
    [Cmdlet("ConvertTo", "OracleInput")]
    public class ConvertToOracleInputCmdlet : PSCmdlet
    {
        //static readonly JsonSerializerOptions _jsonSerializerOptions = new JsonSerializerOptions() { PropertyNameCaseInsensitive = true, AllowTrailingCommas = true, ReadCommentHandling = JsonCommentHandling.Skip };

        [Parameter(ValueFromPipeline = true, ValueFromPipelineByPropertyName = true, HelpMessage = "The result object(StoredProcedureResponse) or JSON from previous database stored procedure or query.")]
        public object DbResult { get { return _spResponse; } set { _spResponse = StoredProcedureResponse.FromObject(value); } }
        protected StoredProcedureResponse _spResponse = new StoredProcedureResponse();

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "Specify which result-set (zero-based ordinal) to be passed to output.")]
        public int ResultSet { get; set; } = 0;

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "(Optional) A dictionary/hashtable or JSON string specifies the mapping from result set column names to input parameter names.")]
        public object MapColToParam
        {
            get
            {
                return _mapColToDict;
            }
            set
            {
                var dict = Transformer.ToDict<string>(value, "MappColToParam");

                if (dict is null)
                {
                    _mapColToDict = new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);
                }
                else
                {
                    _mapColToDict = new Dictionary<string, string>(dict.ToDictionary(x => x.Key, x => x.Value as string), StringComparer.OrdinalIgnoreCase);
                }
            }
        }
        protected IDictionary<string, string> _mapColToDict = new Dictionary<string, string>(0, StringComparer.OrdinalIgnoreCase);

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "(Optional) A dictionary/hashtable or JSON string containing additional parameters to be merged into the input parameters.")]
        public object MergeParams
        {
            get
            {
                return _mergeDict;
            }
            set
            {
                _mergeDict = Transformer.ToDict<object>(value, "MergeParams") ?? new Dictionary<string, object>(0);
            }
        }
        protected IDictionary<string, object> _mergeDict = new Dictionary<string, object>(0);

        [Parameter(ValueFromPipelineByPropertyName = true, HelpMessage = "Indicates that the input value to be overwritten by merging the value if they are the same parameter name.")]
        public bool Override { get; set; } = true;

        [Parameter(HelpMessage = "Whether to convert the result object as a JSON-formatted string.")]
        [Alias("json")]
        public SwitchParameter AsJson { get { return _asJson; } set { _asJson = value; } }
        private bool _asJson = false;

        [Parameter(HelpMessage = "Whether to indent the output JSON text.")]
        public SwitchParameter Indent { get { return _indent; } set { _indent = value; } }
        private bool _indent = false;

        protected override void ProcessRecord()
        {
            var transformed = _spResponse.AsOracleInput(_mapColToDict.AsCaseInsensitive(), ResultSet).Merge(_mergeDict, Override);

            if (_asJson)
            {
                //WriteObject(JsonSerializer.Serialize(transformed, new JsonSerializerOptions { WriteIndented = _indent }));
                WriteObject(JsonConvert.SerializeObject(transformed, _indent ? Formatting.Indented : Formatting.None));
            }
            else
            {
                WriteObject(transformed);
            }
        }
    }
}
