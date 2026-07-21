namespace P2gether.Aos.Rendezvous
{
    /// <summary>Connection and subject values shared with the Java aos-boot service.</summary>
    public sealed class RvClientOptions
    {
        public string Service { get; set; } = "23119";
        public string Network { get; set; } = ";239.11.19.99";
        public string Daemon { get; set; } = "tcp:7500";
        public string Factory { get; set; } = "P2";
        public string Environment { get; set; } = "REAL";
        public string SendSystem { get; set; } = "DOTNET";
        public string Sender { get; set; }
        public bool LocalOnly { get; set; }
    }
}
