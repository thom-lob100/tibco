# aos-boot .NET Framework 4.8 client

`Aos.Rendezvous.sln` contains a reusable C# client and a small console sample. It calls
the Java aos-boot service through Rendezvous; it does not use Java or `tibrvnative.jar`.

## Prerequisites

1. Install the company TIBCO Rendezvous .NET runtime of the same release as the running RV network.
   It provides `TIBCO.Rendezvous.dll` in `<TIBRV_HOME>\bin` and its native DLLs.
2. Set `TIBRV_HOME` to that installation directory, or pass MSBuild
   `/p:TibcoRvHome=<TIBRV_HOME>`.
3. Build **x64** when the installed TIBCO native runtime is 64-bit. The TIBCO `bin` directory
   must be on `PATH` when running the sample.

The project deliberately fails before compilation when `TIBCO.Rendezvous.dll` is missing;
no proprietary DLL is committed to this repository.

## Calling Java aos-boot

`RendezvousClient` creates subjects in the same form as Java:

```
P2.REAL.DOTNET.<host>.SCH.SCH_STATUS
```

`Aos.Rendezvous.Sample` sends `SCH_STATUS` to the Java scheduler (`SCH`) and waits two
seconds for its reply. Change `App.config` to match the actual RV service, network, daemon,
factory, environment and sender values. For an application command, call for example:

```csharp
var reply = client.Request("BOOT", "EQP_STATUS", new Dictionary<string, object>
{
    { "eqpId", "EQP-01" },
    { "portId", "P1" }
}, 2.0);
```

The field names must match the Java command record. `Request` returns `null` only on timeout;
inspect `reply.Status` for Java reply states such as `OK`, `QUEUED`, `NOT_FOUND`, or `ERROR`.
