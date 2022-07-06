// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.IO;
using CommandLine;
using DataBooster.PsInvokeDb.DataAccess;

namespace DataBooster.CmdInvokeDb
{
    class Program
    {
        static int Main(string[] args)
        {
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
                    string resultJson;

                    if (string.IsNullOrEmpty(o.Procedure))
                    {
                        string inputText = GetInputText(o.Query);

                        if (string.IsNullOrWhiteSpace(inputText))
                            throw new ApplicationException("An SQL statement need to be supplied from the file specified by the --query option or from redirected stdin.");

                        resultJson = DbCmd.RunQueryAsJson(o.Server, inputText, o.NamingConvention, o.Indent);
                    }
                    else
                    {
                        resultJson = DbCmd.RunSpAsJson(o.Server, o.Procedure, GetInputText(o.InputJson), o.NamingConvention, o.Indent);
                    }

                    Console.Out.WriteLine(resultJson);
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

        static string GetInputText(string contentOrFileName)
        {
            if (string.IsNullOrEmpty(contentOrFileName))
            {
                return Console.IsInputRedirected ? Console.In.ReadToEnd() : string.Empty;
            }
            else
            {
                return File.Exists(contentOrFileName) ? File.ReadAllText(contentOrFileName) : contentOrFileName;
            }
        }
    }
}
