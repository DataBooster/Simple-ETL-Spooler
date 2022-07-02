// Copyright (c) 2022 Abel Cheng <abelcys@gmail.com> and other contributors to this repository.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// Repository:	https://github.com/DataBooster/Simple-ETL-Spooler

using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Management.Automation;
using Newtonsoft.Json;

namespace DataBooster.PsConvertToOracleInput
{
    public class StoredProcedureResponse
    {
        public IDictionary<string, object>[][] ResultSets { get; set; }
        public IDictionary<string, object> OutputParameters { get; set; }
        public object ReturnValue { get; set; }

        public StoredProcedureResponse()
        {
            ResultSets = new IDictionary<string, object>[0][];
            OutputParameters = new Dictionary<string, object>(0);
        }

        public static StoredProcedureResponse FromObject(object source)
        {
            object inputObject = (source is PSObject psObject) ? psObject.BaseObject : source;

            switch (inputObject)
            {
                case null:
                    return new StoredProcedureResponse();
                case StoredProcedureResponse spResp:
                    return spResp;
                case string json:
                    return JsonConvert.DeserializeObject<StoredProcedureResponse>(json) ?? new StoredProcedureResponse();
                default:
                    Type inType = inputObject.GetType();

                    if (inType.IsPrimitive || inType.IsEnum || inType.Equals(typeof(decimal)))
                    {
                        throw new ArgumentException("Only accepts input of type StoredProcedureResponse.", "DbResult");
                    }
                    else if (inType.Name == "StoredProcedureResponse")
                    {
                        dynamic spResp = inputObject;
                        return new StoredProcedureResponse()
                        {
                            ResultSets = ((IEnumerable)spResp.ResultSets).Cast<IEnumerable>().Select(x => x.Cast<IDictionary<string, object>>().ToArray()).ToArray(),
                            OutputParameters = spResp.OutputParameters,
                            ReturnValue = spResp.ReturnValue
                        };
                    }
                    else
                    {
                        string json = JsonConvert.SerializeObject(inputObject);
                        return JsonConvert.DeserializeObject<StoredProcedureResponse>(json) ?? new StoredProcedureResponse();
                    }
            }
        }
        /*
        public bool IsEmpty()
        {
            if ((ResultSets == null || ResultSets.Length == 0) &&
                (OutputParameters == null || OutputParameters.Count == 0) &&
                ReturnValue == null)
                return true;
            else
                return false;
        }
        */
    }
}
