using System.Collections.Generic;

namespace DataBooster.PsInvokeMdx
{
    public class StoredProcedureResponse
    {
        public IList<IList<IDictionary<string, object>>> ResultSets { get; set; }
        public IDictionary<string, object> OutputParameters { get; set; }
        public object ReturnValue { get; set; }

        public StoredProcedureResponse()
        {
            ResultSets = new List<IList<IDictionary<string, object>>>();
            OutputParameters = new Dictionary<string, object>(0);
        }
    }
}
