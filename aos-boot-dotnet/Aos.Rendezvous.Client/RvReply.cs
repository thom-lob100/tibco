using System.Collections.Generic;

namespace P2gether.Aos.Rendezvous
{
    public sealed class RvReply
    {
        internal RvReply(IDictionary<string, object> fields)
        {
            Fields = new Dictionary<string, object>(fields);
        }

        public IDictionary<string, object> Fields { get; private set; }

        public string Status
        {
            get
            {
                object value;
                return Fields.TryGetValue("status", out value) ? value as string ?? value.ToString() : null;
            }
        }
    }
}
