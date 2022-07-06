// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.IO;
using CommandLine;
using DataBooster.PsConvertToOracleInput;

namespace DataBooster.CmdConvertToOracleInput
{
    class Program
    {
        static int Main(string[] args)
        {
            string ReadJsonFile(string jsonFile)
            {
                return string.IsNullOrWhiteSpace(jsonFile) ? "{}" : File.ReadAllText(jsonFile);
            }

            Console.Out.WriteLine();

            var cmdParser = new Parser(settings =>
            {
                settings.CaseSensitive = false;
                settings.CaseInsensitiveEnumValues = true;
                settings.HelpWriter = Console.Error;
            });

            int exitCode = 1;
            var parseResult = cmdParser.ParseArguments<CmdOptions>(args);

            parseResult.WithParsed<CmdOptions>(o =>
            {
                try
                {
                    string resultJson = Console.IsInputRedirected ? Console.In.ReadToEnd() : string.Empty;
                    string mapJson = ReadJsonFile(o.MappColToParam);
                    string mergeJson = ReadJsonFile(o.MergeJson);

                    if (o.CheckPipe && string.IsNullOrWhiteSpace(resultJson))
                        throw new MissingFieldException("Unable to receive valid result data from the previous node of the pipeline.");

                    Console.Out.WriteLine(Transformer.ToOracleInputJson(resultJson, o.ResultSet, mapJson, mergeJson, o.Override, o.Indent));
                    exitCode = 0;
                }
                catch (Exception e)
                {
                    string errMsg = e.Message;
                    if (e.InnerException != null && !string.IsNullOrEmpty(e.InnerException.Message))
                        if (string.IsNullOrEmpty(errMsg))
                            errMsg = e.InnerException.Message;
                        else
                            errMsg += e.InnerException.Message;
                    if (string.IsNullOrWhiteSpace(errMsg))
                        errMsg = e.ToString();

                    Console.Error.Write(e.GetType().Name + ": ");
                    Console.Error.WriteLine(errMsg);
                }
            });

            return exitCode;
        }
    }
}
