// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using CommandLine;
using CommandLine.Text;
using System.Collections.Generic;

namespace DataBooster.CmdConvertToOracleInput
{
    class CmdOptions
    {
        [Option('c', "checkpipe", Default = false, HelpText = "Check the input pipeline first. Just exit without doing anything if the input pipe is empty.")]
        public bool CheckPipe { get; set; }

        [Option('r', "resultset", Default = 0, HelpText = "Specify which result-set (0-based index) to be passed to output.")]
        public int ResultSet { get; set; }

        [Option('m', "map", HelpText = "(Optional) A The JSON file specifies the mapping from result set column names to input parameter names.")]
        public string MappColToParam { get; set; } = string.Empty;

        [Option('y', "merge", HelpText = "(Optional) A JSON file that will be merged into the input JSON redirected from the console pipeline.")]
        public string MergeJson { get; set; } = string.Empty;

        [Option('o', "override", Default = true, HelpText = "Indicates that the input value to be overwritten by merging the value if they are the same parameter name.")]
        public bool Override { get; set; }

        [Option('i', "indent", Default = false, HelpText = "(For Oracle only) Specify which result-set (0-based index) to be passed to output.")]
        public bool Indent { get; set; }

        [Usage()]
        public static IEnumerable<Example> Examples
        {
            get
            {
                yield return new Example("\nTransform JSON from a stored procedure's result to input parameters for another stored procedure.\n" +
                    "  Example",
                    new CmdOptions { ResultSet = 2, MappColToParam = "map columns to parameters.json", MergeJson = "test merge.json" });
            }
        }
    }
}
