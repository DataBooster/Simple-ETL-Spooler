// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Collections.Generic;
using CommandLine;
using CommandLine.Text;

namespace DataBooster.CmdInvokeDb
{
    /*
    public enum ServerType
    {
        Auto,
        Oracle,
        SqlServer
    }
    */

    public class CmdOptions
    {
        [Option('s', "server", Required = true, HelpText = "The database (Oracle or SQL Server) server to connect to. E.g. EDBXYZ, SQL1XYZ")]
        public string Server { get; set; }

        [Option('p', "procedure", SetName = "sp", HelpText = "The fully qualified name of the stored procedure to call. E.g. for Orale: \"SCHEMA.PACKAGE.PING_SP\", for SQL Server: \"db_name.dbo.ping_sp\"\n(--procedure option and --dynamic option are mutually exclusive)")]
        public string Procedure { get; set; }

        [Option('i', "input", SetName = "sp", HelpText = "(Optional) A JSON file that contains all the input parameters required by the stored procedure.\nAlternatively, the input JSON can be redirected from the console pipeline.")]
        public string InputJson { get; set; }

        //[Option('d', "dynamic", SetName = "dq", Required = true, HelpText = "This switch indicates that the input file or redirected stdin will contain a dynamic SQL statement.\n(--dynamic option and --procedure option are mutually exclusive)")]
        //public bool IsDynamicQuery { get; set; }

        [Option('q', "query", SetName = "dq", HelpText = "A text file that contains a dynamic SQL statement.\nAlternatively, the dynamic SQL can be redirected from the console pipeline.")]
        public string Query { get; set; }

        /*
        [Option('t', "type", Default = ServerType.Auto, HelpText = "Specify the database type: O - Oracle; S - SqlServer; A - Automatic selection based on the server name prefix (\"ED...\" - Oracle; others - SqlServer).")]
        public ServerType DbType { get; set; }
        public bool IsOracle
        {
            get
            {
                switch (DbType)
                {
                    case ServerType.Auto:
                        return Server.StartsWith("ED", StringComparison.OrdinalIgnoreCase);
                    case ServerType.Oracle:
                        return true;
                    default:
                        return false;
                }
            }
        }
        */

        [Option('n', "naming", Default = "PascalCase", HelpText = "Specifies the naming convention for the properties of returning result-sets.\nN or None (as it is in database); P or PascalCase; c or camelCase;")]
        public string NamingConvention { get; set; }

        [Option("indent", Default = false, HelpText = "Whether to indent the output JSON text.")]
        public bool Indent { get; set; }

        [Usage()]
        public static IEnumerable<Example> Examples
        {
            get
            {
                yield return new Example("\n==Stored Procedure Mode==\n  Connect to an Oracle or SQL Server database and execute a stored procedure or function generically. The result will be rendered as JSON on stdout.\n" +
                    "  Notes: The current user (you) needs to have execute permission on the package of the stored procedure or function.\n\n" +
                    "  Example",
                    new CmdOptions { Server = "EDBXYZ", Procedure = "SCHEMA.EDB_PING.GET_DB_NAME", InputJson = "test input.json" });

                yield return new Example("\n==Dynamic SQL Mode==\n  Connect to an Oracle or SQL Server database and execute a dynamic SQL generically. The result will be rendered as JSON on stdout.\n" +
                    "  Notes: The current user (you) needs the corresponding permissions to run the query.\n\n" +
                    "  Example",
                    new CmdOptions { Server = "EDBXYZ", Query = "test query.sql" });
            }
        }
    }
}
