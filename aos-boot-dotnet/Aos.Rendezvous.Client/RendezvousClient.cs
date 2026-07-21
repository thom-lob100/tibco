using System;
using System.Collections.Generic;
using System.Net;
using TIBCO.Rendezvous;
using RvEnvironment = TIBCO.Rendezvous.Environment;

namespace P2gether.Aos.Rendezvous
{
    /// <summary>
    /// .NET Framework client for Java aos-boot's six-element subject convention:
    /// factory.environment.send-system.sender.listener.command.
    /// </summary>
    public sealed class RendezvousClient : IDisposable
    {
        private readonly RvClientOptions options;
        private readonly NetTransport transport;
        private bool disposed;

        public RendezvousClient(RvClientOptions options)
        {
            if (options == null) throw new ArgumentNullException(nameof(options));
            Validate(options);

            this.options = options;
            RvEnvironment.Open();
            try
            {
                transport = new NetTransport(options.Service, options.Network, options.Daemon);
                transport.Description = "aos-dotnet-client/" + options.SendSystem + "/" + Sender;
            }
            catch
            {
                RvEnvironment.Close();
                throw;
            }
        }

        public string Sender
        {
            get { return string.IsNullOrWhiteSpace(options.Sender) ? Dns.GetHostName() : options.Sender; }
        }

        public void Publish(string listener, string command, IDictionary<string, object> fields)
        {
            Message message = CreateMessage(listener, command, fields);
            try
            {
                transport.Send(message);
            }
            finally
            {
                message.Dispose();
            }
        }

        /// <summary>
        /// Sends an RV request and returns null only on timeout. Java maps failures to
        /// reply field status values such as ERROR, QUEUED, or NOT_FOUND.
        /// </summary>
        public RvReply Request(string listener, string command, IDictionary<string, object> fields,
                               double timeoutSeconds)
        {
            Message request = CreateMessage(listener, command, fields);
            try
            {
                Message reply = transport.SendRequest(request, timeoutSeconds);
                if (reply == null) return null;
                try
                {
                    return new RvReply(ReadFields(reply));
                }
                finally
                {
                    reply.Dispose();
                }
            }
            finally
            {
                request.Dispose();
            }
        }

        private Message CreateMessage(string listener, string command, IDictionary<string, object> fields)
        {
            if (string.IsNullOrWhiteSpace(listener)) throw new ArgumentException("Listener is required.", nameof(listener));
            if (string.IsNullOrWhiteSpace(command)) throw new ArgumentException("Command is required.", nameof(command));

            var message = new Message { SendSubject = SubjectFor(listener, command) };
            if (fields == null) return message;

            foreach (var field in fields)
            {
                if (string.IsNullOrWhiteSpace(field.Key)) throw new ArgumentException("A field name is required.", nameof(fields));
                if (field.Value != null) AddField(message, field.Key, field.Value);
            }
            return message;
        }

        private IDictionary<string, object> ReadFields(Message message)
        {
            var fields = new Dictionary<string, object>(StringComparer.Ordinal);
            for (uint index = 0; index < message.FieldCount; index++)
            {
                MessageField field = message.GetFieldByIndex(index);
                fields[field.Name] = field.Value;
            }
            return fields;
        }

        private static void AddField(Message message, string name, object value)
        {
            if (value is string) message.AddField(name, (string)value);
            else if (value is bool) message.AddField(name, (bool)value);
            else if (value is sbyte) message.AddField(name, (sbyte)value);
            else if (value is byte) message.AddField(name, (byte)value);
            else if (value is short) message.AddField(name, (short)value);
            else if (value is ushort) message.AddField(name, (ushort)value);
            else if (value is int) message.AddField(name, (int)value);
            else if (value is uint) message.AddField(name, (uint)value);
            else if (value is long) message.AddField(name, (long)value);
            else if (value is ulong) message.AddField(name, (ulong)value);
            else if (value is float) message.AddField(name, (float)value);
            else if (value is double) message.AddField(name, (double)value);
            else if (value is DateTime) message.AddField(name, (DateTime)value);
            else if (value is byte[]) message.AddField(name, (byte[])value);
            else throw new ArgumentException("Unsupported RV field type: " + value.GetType().FullName);
        }

        private string SubjectFor(string listener, string command)
        {
            var subject = string.Join(".", options.Factory, options.Environment, options.SendSystem,
                                      Sender, listener, command);
            return options.LocalOnly ? "_LOCAL." + subject : subject;
        }

        public void Dispose()
        {
            if (disposed) return;
            disposed = true;
            if (transport != null) transport.Destroy();
            RvEnvironment.Close();
        }

        private static void Validate(RvClientOptions options)
        {
            if (string.IsNullOrWhiteSpace(options.Service)) throw new ArgumentException("Service is required.");
            if (string.IsNullOrWhiteSpace(options.Network)) throw new ArgumentException("Network is required.");
            if (string.IsNullOrWhiteSpace(options.Daemon)) throw new ArgumentException("Daemon is required.");
            if (string.IsNullOrWhiteSpace(options.Factory)) throw new ArgumentException("Factory is required.");
            if (string.IsNullOrWhiteSpace(options.Environment)) throw new ArgumentException("Environment is required.");
            if (string.IsNullOrWhiteSpace(options.SendSystem)) throw new ArgumentException("SendSystem is required.");
        }
    }
}
