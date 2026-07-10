# aos-boot

Minimal Spring Boot multi-module project whose instances join a TIBCO Rendezvous
**distributed queue (DQ)**: run the same app twice and each message on the subject is
processed by exactly one instance, with automatic scheduler election and failover.

> **상세 사용법·회사(실 TIBCO 환경) 적용 체크리스트는 [USAGE.md](USAGE.md) 참고.**

- `aos-boot` — parent POM (Spring Boot 4.1.0, JDK 25, Lombok)
- `aos-boot-app` — bootstrap module; joins DQ `AOS.BOOT.DQ` on subject `P2.TEST.*.*.BOOT.*`
  (service `23119`, network `;239.11.19.99`, daemon `tcp:7500`) and logs each assigned message
- `../tibrv-stub` — in-memory stub of `tibrvj` for machines without a TIBCO install:
  compiles the app AND simulates Rendezvous within one JVM (subject matching, DQ
  round-robin, request/reply) so logic can be tested locally; it cannot reach a real rvd

## Prerequisites

1. JDK 25, Maven, Eclipse (m2e + Lombok plugin installed in the IDE).
2. Without a TIBCO install: `mvn -f ../tibrv-stub/pom.xml clean install` once — the app then
   compiles and can be run against the JVM-local in-memory bus (single process only; a
   `[tibrvj-stub]` warning is printed at startup).
3. To **run against real Rendezvous**, the genuine `tibrvj.jar` from a TIBCO Rendezvous 8.4.x
   installation must replace the stub in the local Maven repository:

   ```
   mvn install:install-file -Dfile="<TIBRV_HOME>/lib/tibrvj.jar" ^
       -DgroupId=com.tibco.tibrv -DartifactId=tibrvj -Dversion=8.4.5 -Dpackaging=jar
   ```

   `<TIBRV_HOME>\bin` must also be on `PATH` (native libraries), and the `rvd` daemon
   reachable at `tcp:7500`.

## Run two DQ members in Eclipse

1. `File > Import > Existing Maven Projects` and select this folder.
2. Create two Run Configurations for `AosBootApplication`, differing only in program arguments:
   - instance A (preferred scheduler): `--aos.rendezvous.dq.scheduler-weight=2`
   - instance B (worker): `--aos.rendezvous.dq.scheduler-weight=1`
3. Start both. They join the same DQ group `AOS.BOOT.DQ`; the scheduler assigns each message on
   `P2.TEST.*.*.BOOT.*` to one member only. Kill either instance and the group keeps working.

Worker capacity is tunable via `aos.rendezvous.dq.worker-weight` / `worker-tasks`
(defaults: 1 / 1, set in `RendezvousProperties.Dq`).

## Subject structure and per-service DQ groups

The subject is composed from six elements under `aos.rendezvous.subject.*`:

| Element       | Default | Meaning                            |
|---------------|---------|------------------------------------|
| `factory`     | `P2`    | factory code                       |
| `environment` | —       | from the active profile (below)    |
| `send-system` | `*`     | sending system                     |
| `sender`      | `*`     | sender                             |
| `listener`    | `BOOT`  | this service (per-launch element)  |
| `command`     | `*`     | command                            |

`aos.rendezvous.subject.local=true` prepends the reserved `_LOCAL` element
(`_LOCAL.P2.TEST.*.*.BOOT.*`); Rendezvous delivers such subjects only within the
local rvd daemon, never onto the network.

The DQ group name is derived from the listener element as `AOS.<listener>.DQ`, so one
argument switches both the subject and the DQ group:

- default: subject `P2.TEST.*.*.BOOT.*`, DQ `AOS.BOOT.DQ`
- OIS role: `--aos.rendezvous.subject.listener=OIS` → subject `P2.TEST.*.*.OIS.*`, DQ `AOS.OIS.DQ`

`--aos.rendezvous.dq.name=...` still overrides the derived name directly if needed.
Note: two DQ groups listening on the same subject each process every message once
(one delivery per group), so distinct services need distinct listener elements.

## Command dispatch (@RvCommand)

Each message assigned to this DQ member is routed by its **command element** (the last
subject element) to a matching handler method — annotate any Spring bean method with
`@RvCommand`. Handlers work with **typed request/reply messages**: declare a record
and its components are bound from the message fields by name (raw `TibrvMsg` also
works, e.g. when the payload is one full-document field):

```java
@Component
public class SampleCommands {

    public record OrderCreateRequest(String orderId, int quantity) {}
    public record OrderCreateReply(String status, String handledBy, String orderId) {}

    @RvCommand                       // command ORDER_CREATE, derived from the method name
    public OrderCreateReply orderCreate(OrderCreateRequest request) {
        return new OrderCreateReply("OK", "orderCreate", request.orderId());
    }                                // sent back automatically on request/reply calls

    @RvCommand("PING")               // explicit command name; void = never replies
    public void ping(TibrvMsg request) { ... }
}
```

Handlers take one parameter — a record or `TibrvMsg` — or none, and return a record,
`TibrvMsg`, or `void`. A returned value is sent back when the request carries a reply
subject, so request/reply works end to end without touching transports. Handlers are
collected at startup (`RvCommandDispatcher`); duplicate command names fail fast.

**Failure policy** (`aos.rendezvous.handler-retry.*`, defaults `retries: 2`,
`backoff: 0.5`s, `threads: 2`): the first attempt runs in the DQ callback; when it
throws, the callback returns immediately and retries run **asynchronously** on the
`rv-handler-retry-*` pool, so the member keeps consuming new messages while a failed
one is retried. A retry that succeeds sends its reply as usual; after the last failure
— or when no handler exists for the command — a `{status=ERROR, command, error}` reply
is sent to request/reply callers so they fail fast instead of waiting out their
timeout. Exceptions never reach the Rendezvous callback, so the DQ assignment stays
confirmed and the message is not redelivered (a crash mid-retry loses pending retries;
retried handlers must also tolerate running in parallel with newer messages). Keep the
total retry duration (`retries × backoff` + handler time) below callers'
`aos.rendezvous.request.timeout`, or the caller times out and resends before the
ERROR reply arrives.

### Durable commands (DB-backed reprocessing queue)

For commands that must not be lost, mark the handler `@RvCommand(durable = true)`
(e.g. `orderSettle` in `SampleCommands`). The message is then **persisted to the
`rv_command_queue` table before processing** (see `schema.sql`):

```java
@Transactional(rollbackFor = Exception.class)              // settle write + submit are
@RvCommand(durable = true)                                 // ONE transaction
public TibrvMsg orderSettle(OrderSettleRequest request) throws TibrvException {
    settleInDb(request);                                   // business write, runs once
    durableQueue.submit("NOTIFY_SETTLED",                  // chain the notification as
            new OrderSettledEvent(request.orderId(),       // its own durable command
                    "SETTLED", "orderSettle"));
    ...                                                    // reply OK - settle is done
}

@RvCommand(value = "NOTIFY_SETTLED", durable = true)       // publish-only step,
public void notifySettled(OrderSettledEvent event) throws TibrvException {
    publisher.publish("messo", "ORDER_SETTLED", event);    // retried independently
}
```

**Chaining durable steps**: `RvDurableCommandQueue.submit(command, payload)` inserts a
follow-up command directly into the queue (no RV round trip). Splitting settle and
notify this way means a failed MESSO publish retries only `NOTIFY_SETTLED` — the
settlement is never re-executed — and the notification still cannot be lost (it is a
persisted row from the moment settle commits). Publishing inline inside the handler is
also possible but couples the steps: the queue would then retry settle + notify
together, requiring the settlement itself to be idempotent.

**Atomicity**: with `@Transactional` on the handler, the business write and the
`submit` INSERT share one transaction (same datasource) — a failure rolls back both, so
a settlement can never exist without its notification row or vice versa. Two caveats:
use `rollbackFor = Exception.class`, because `submit` throws the checked
`TibrvException` which the default rollback rule ignores; and `@Transactional` works
here because the dispatcher invokes handlers through the Spring proxy. The handler's
own queue row bookkeeping (enqueue/complete/fail) intentionally stays outside the
transaction, so a rolled-back attempt is still recorded and retried. A publish with no
listeners does not fail — RV publish only throws on transport/config errors.

- inline first attempt succeeds → row deleted, normal reply;
- it fails → the row stays queued and the caller immediately receives
  `{status=QUEUED, queueId, error}` — the message is safe and will be retried;
- a background poller (`rv-durable-queue` thread) re-runs due rows with backoff —
  **surviving JVM restarts** — until success or `max-attempts`, then keeps the row as
  `FAILED` (dead letter). Requeue manually with
  `UPDATE rv_command_queue SET status = 'PENDING', attempts = 0 WHERE id = ...`.
- rows stuck in `PROCESSING` (worker crashed mid-attempt) are reclaimed after
  `processing-timeout`; claiming uses conditional UPDATEs, so multiple instances can
  share one queue table.

Durable retries never send replies (the requester's inbox is gone by then); treat
durable commands as async-acknowledged. Tune under `aos.rendezvous.durable-queue.*`
(`enabled`, `poll-interval: 5`s, `max-attempts: 5`, `backoff: 10`s, `batch-size`,
`processing-timeout: 300`s). The queue is backed by `spring.datasource` — a local H2
file database (`./data/rv-command-queue`) by default; point it at the real database in
production. Field values are persisted as strings (record binding converts them back),
so durable handlers should accept string-convertible field types.

## Calling other services (destinations)

Outbound targets such as MESSO are predefined under `aos.rendezvous.destinations.<name>`;
the map holds any number of destinations, each with its own `service` / `network` /
`daemon` (omitted values fall back to this app's own connection) and the target's
`listener` element:

```yaml
destinations:
  messo:
    listener: "MESSO"
    service: "23121"
    network: ";239.11.19.98"
  bill:
    listener: "BILL"
    service: "23122"
```

`RendezvousPublisher` keeps one transport per destination and composes the outbound
subject per call as `factory.environment.<send-system>.<sender>.<target listener>.<command>`,
where `send-system` defaults to this app's own listener element (the calling service) and
`sender` defaults to the machine name (override with `aos.rendezvous.sender-name`, e.g.
when several instances share a host). Factory, environment and `_LOCAL` follow the active
profile, so a QA instance automatically calls MESSO with a QA subject.

Payloads may be a record, a `Map`, or a raw `TibrvMsg`; replies can be bound into a
record by passing its class:

```java
// one-way (publish)
publisher.publish("messo", "ORDER_CREATE", new OrderCreateRequest("ORD-1", 3));
// request/reply with typed binding — null when every attempt times out
OrderCreateReply reply = publisher.request("messo", "ORDER_CREATE",
        Map.of("orderId", "ORD-1", "quantity", 3), OrderCreateReply.class);
```

**Timeout policy** (`aos.rendezvous.request.*`, defaults `timeout: 5.0`s per attempt,
`retries: 2`, `backoff: 0.5`s): a timed-out request is resent, and `request` returns
`null` only after every attempt timed out. Retries resend the same request, so target
handlers should be idempotent for retried commands.

E.g. a BOOT instance on host `EAPHOST01` on the `test` profile sends to
`P2.TEST.BOOT.EAPHOST01.MESSO.ORDER_CREATE`.

To exercise request/reply without the real target system, start with
`--aos.rendezvous.destination-simulator.enabled=true`: `DestinationSimulator`
(disabled by default) impersonates one destination — `messo` unless overridden with
`--aos.rendezvous.destination-simulator.destination=<name>` — by listening on its
connection for `factory.environment.*.*.<listener>.>` and replying `{status=OK}` to
every request. It works both against the in-memory stub (same JVM) and a real rvd
(separate processes).

## Environments (Spring profiles)

The `environment` element comes from the active profile: `application-test.yml` (`TEST`),
`application-qa.yml` (`QA`), `application-real.yml` (`REAL`). The default profile is
`test`; switch with e.g. `--spring.profiles.active=real`. Per-environment connection
settings (daemon, network, service) belong in the matching `application-<profile>.yml`.

Example — QA OIS-role instance, local-only delivery:

```
--spring.profiles.active=qa --aos.rendezvous.subject.listener=OIS --aos.rendezvous.subject.local=true
```

→ subject `_LOCAL.P2.QA.*.*.OIS.*`, DQ `AOS.OIS.DQ`.
