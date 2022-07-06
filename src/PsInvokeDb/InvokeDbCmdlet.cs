// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Data;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Management.Automation;
using DbParallel.DataAccess;
using DataBooster.PsInvokeDb.DataAccess;
using Newtonsoft.Json;

namespace DataBooster.PsInvokeDb
{
    [Cmdlet("Invoke", "DB")]
    [OutputType(typeof(StoredProcedureResponse))]
    [Alias("Call-DB")]
    public class InvokeDbCmdlet : PSCmdlet
    {
        [Parameter(Mandatory = true, HelpMessage = "The database (Oracle or SQL Server) server to connect to. E.g. EDBXYZ or SQLXYZ")]
        public string Server { get; set; }

        [Parameter(ParameterSetName = "StoredProcedure", Mandatory = true, ValueFromPipelineByPropertyName = true, HelpMessage = "The fully qualified name of the stored procedure to call. E.g. for Orale: \"SCHEMA.PACKAGE.PING_SP\", for SQL Server: \"db_name.dbo.ping_sp\"\n(--procedure option and --dynamic option are mutually exclusive)")]
        [Alias("sp")]
        public string Procedure { get; set; }

        [Parameter(ParameterSetName = "StoredProcedure", ValueFromPipeline = true, ValueFromPipelineByPropertyName = true, HelpMessage = "(Optional) A hashtable that contains all the input parameters required by the stored procedure.\nAlternatively, the hashtable can be redirected from the pipeline.")]
        [Alias("input")]
        public object InputParameters { get { return _inputDict; } set { _inputDict = ToDict(value, "InputParameters", Procedure); } }
        protected IDictionary<string, object> _inputDict;
        public static IDictionary<string, object> ToDict(object inputObject, string argName, string procedure)
        {
            if (inputObject is PSObject psObject)
                inputObject = psObject.BaseObject;

            switch (inputObject)
            {
                case IDictionary<string, object> dict:
                    return dict;
                case IDictionary hashTable:
                    return hashTable.Cast<DictionaryEntry>().ToDictionary(x => (string)x.Key, x => x.Value);
                case string json:
                    return JsonConvert.DeserializeObject<IDictionary<string, object>>(json);
                case null:
                    return null;
                default:
                    Type inType = inputObject.GetType();

                    if (inType.IsPrimitive || inType.IsEnum || inType.Equals(typeof(decimal)))
                    {
                        throw new ArgumentException("Only accepts a dictionary, hashtable, JSON string or anonymous object.", argName);
                    }
                    else
                    {
                        StoredProcedureRequest spReq = new StoredProcedureRequest(procedure, inputObject);
                        return spReq.InputParameters;
                    }
            }
        }

        [Parameter(ParameterSetName = "DynamicQuery", Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true, HelpMessage = "A dynamic SQL query to be executed.")]
        public string Query { get; set; }

        /*
        [Parameter(HelpMessage = "Specify the database type: O - Oracle; S - SqlServer; A - Automatic selection based on the server name prefix (\"ED...\" - Oracle; others - SqlServer).")]
        [ValidateSet("Oracle", "SqlServer", "Auto", "SQL", "O", "S", "A", IgnoreCase = false)]
        public string DbType { get; set; } = "Auto";
        internal bool IsOracle
        {
            get
            {
                switch (char.ToUpper(DbType[0]))
                {
                    case 'S': return false;
                    case 'A': return Customizer.IsOracle(Server);
                    default: return true;
                }
            }
        }
        */

        [Parameter(HelpMessage = "Specifies the naming convention for the properties of returning result-sets.\nN or None (as it is in database); P or PascalCase; c or camelCase;")]
        [ValidateSet("PascalCase", "CamelCase", "None", "Pascal", "Camel", "P", "C", "N", IgnoreCase = false)]
        [Alias("naming")]
        public string NamingConvention { get; set; } = "PascalCase";

        [Parameter(HelpMessage = "Whether to convert the result object as a JSON-formatted string.")]
        [Alias("json")]
        public SwitchParameter AsJson { get { return _asJson; } set { _asJson = value; } }
        private bool _asJson = false;

        [Parameter(HelpMessage = "Whether to indent the output JSON text.")]
        public SwitchParameter Indent { get { return _indent; } set { _indent = value; } }
        private bool _indent = false;

        protected override void ProcessRecord()
        {
            StoredProcedureResponse dbResult;

            if (ParameterSetName == "StoredProcedure")
            {
                dbResult = DalCenter.Execute(Server, Procedure, CommandType.StoredProcedure, _inputDict, NamingConvention);
            }
            else
            {
                dbResult = DalCenter.Execute(Server, Query, CommandType.Text, null, NamingConvention);
            }

            if (_asJson)
            {
                WriteObject(JsonConvert.SerializeObject(dbResult, _indent ? Formatting.Indented : Formatting.None));
            }
            else
            {
                WriteObject(dbResult);
            }
        }
    }
}
