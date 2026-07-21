using System;
using System.Collections.Generic;
using System.Configuration;
using P2gether.Aos.Rendezvous;

namespace P2gether.Aos.Rendezvous.Sample
{
    internal static class Program
    {
        private static int Main()
        {
            try
            {
                using (var client = new RendezvousClient(new RvClientOptions
                {
                    Service = Setting("rv.service"),
                    Network = Setting("rv.network"),
                    Daemon = Setting("rv.daemon"),
                    Factory = Setting("rv.factory"),
                    Environment = Setting("rv.environment"),
                    SendSystem = Setting("rv.sendSystem"),
                    Sender = Setting("rv.sender")
                }))
                {
                    var reply = client.Request("SCH", "SCH_STATUS", null, 2.0);
                    Console.WriteLine(reply == null ? "RV timeout" : "status=" + reply.Status);
                    return reply == null ? 2 : 0;
                }
            }
            catch (Exception exception)
            {
                Console.Error.WriteLine(exception);
                return 1;
            }
        }

        private static string Setting(string key)
        {
            return ConfigurationManager.AppSettings[key];
        }
    }
}
