// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System.Collections.Generic;
using CommandLine;
using CommandLine.Text;

namespace DataBooster.CmdInvokeMdx
{
    public class CmdOptions
    {
        [Option('c', "connection", Required = true, HelpText = "The full connection string used to specify the analytics data source. E.g. \"Provider=MSOLAP;Data Source=http://yourolap/xyzcube/;Initial Catalog=YourCube;\"")]
        public string ConnectionString { get; set; }

        [Option('q', "query", HelpText = "A text file that contains a MDX query.\nAlternatively, the MDX query can be redirected from the console pipeline.")]
        public string MdxQuery { get; set; }

        [Option('f', "fullcolumn", Default = false, HelpText = "Whether to return the raw full column names? By default, only shortened column names are returned.")]
        public bool FullColumnName { get; set; }

        [Option("indent", Default = false, HelpText = "Whether to indent the output JSON text.")]
        public bool Indent { get; set; }

        [Usage()]
        public static IEnumerable<Example> Examples
        {
            get
            {
                yield return new Example("Connects to an AnalysisServices and executes a MDX query. The result will be rendered as JSON on stdout.\n" +
                    "  Example",
                    new CmdOptions { ConnectionString = "Provider=MSOLAP;Data Source=http://yourolap/xyzcube/;Initial Catalog=YourCube;", MdxQuery = "SELECT NON EMPTY { [Measures].[Market Value] } ON COLUMNS, NONEMPTY( [Value Date].[Date].[All].children) ON ROWS FROM[Market]" });
            }
        }
    }
}
