# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository layout

This is a workspace with two sibling Maven projects (there is no root pom):

- `aos-boot/` — Spring Boot 4.1 multi-module project: a TIBCO Rendezvous (RV) distributed-queue service skeleton.
  - `aos-boot-core` — the RV framework library (`com.p2gether.aos.rv`): DQ subscriber, `@RvCommand` dispatch, publisher/destinations, DB-backed persistent command queue, FT coordination, `rv_command_queue` DDL.
  - `aos-boot-application` — the **only runnable/deployable unit** (`AosBootApplication` + profile ymls, `-exec` classified jar, no sample code). One jar serves every role: the launch argument `--aos.rendezvous.subject.listener=<ROLE>` decides what an instance is, and production runs one folder per role with role-specific arguments (managed by an external service-manager program).
  - `aos-boot-scheduler` — **library module** for the scheduler role: beans exist only when launched with listener `SCH` (`SchedulerConfiguration` gate), so scheduled work cannot leak into other roles. The SCH role must be launched with FT enabled (single active) and usually `--spring.main.web-application-type=none`. Spring `@Scheduled` still runs on FT standbys — every periodic job must start with a `RendezvousSubscriber.isActive()` guard.
  - `aos-boot-samples` — demo handlers (`SampleCommands`, `EqpCommands`), REST gateway sample (`EqpApiController`), seed data. Excluded from the production artifact.
- `tibrv-stub/` — standalone project producing a fake `com.tibco.tibrv:tibrvj:8.4.5`. Deliberately **outside the aos-boot reactor** so a normal build can never shadow a real tibrvj. Two roles: compile-only API signatures, plus an in-memory single-JVM RV simulation (subject matching, DQ round-robin, `_INBOX` request/reply) so the app actually runs without TIBCO.

Detailed docs (keep them updated when behavior changes): `aos-boot/README.md` (English, feature reference), `aos-boot/USAGE.md` (Korean, config reference + production adoption checklist), `tibrv-stub/README.md`.

## Build and run

`tibrvj.jar` is proprietary and not on Maven Central — nothing compiles until the stub (or a real jar) is in the local repo:

```bash
mvn -f tibrv-stub/pom.xml clean install    # once per machine
mvn -f aos-boot/pom.xml compile
mvn -f aos-boot/pom.xml package            # aos-boot-application-*-exec.jar = production artifact
```

- Poms target **JDK 25**; on a machine with an older JDK add e.g. `-Dmaven.compiler.release=17` (how this repo was developed and verified).
- There are **no tests yet** (`src/test` is empty everywhere); verification is done by running the app against the stub.
- Run the demo (samples included): `mvn -f aos-boot/pom.xml -pl aos-boot-samples spring-boot:run`
- The stub bus is **single-JVM only**. Multi-instance behavior (two DQ members, FT failover) requires a real rvd, or two Run Configurations in an IDE won't share the stub bus — see README "Run two DQ members in Eclipse" (real RV needed for cross-process).
- Startup log `[tibrvj-stub]` means the stub is on the classpath — must never appear in production logs.

### tibrv-stub maintenance rule

If you add or change a TIBCO API call in `aos-boot`, mirror the signature in `tibrv-stub/src/main/java/com/tibco/tibrv/` or compilation breaks. When editing the stub, reinstall it (`mvn -f tibrv-stub/pom.xml clean install`) before rebuilding aos-boot.

## Architecture

All messaging flows through a 6-element subject `factory.environment.send-system.sender.listener.command` (default subscribe pattern `P2.TEST.*.*.BOOT.*`). One rule regardless of direction: element 3 = sending service, 4 = sending host, 5 = receiving service (listener), 6 = command. The `environment` element comes from the active Spring profile (`test`/`qa`/`real` → `TEST`/`QA`/`REAL`); the DQ group name (`AOS.<listener>.DQ`) and FT group name (`AOS.<listener>.FT`) are derived from the listener element, so `--aos.rendezvous.subject.listener=OIS` re-roles an instance in one argument.

Inbound path: `RendezvousSubscriber` (joins the DQ; each message goes to exactly one group member) → `RvCommandDispatcher` routes by the last subject element to a `@RvCommand` bean method. Handlers take a record (fields bound by name) or raw `TibrvMsg`; a returned value is auto-replied when the request carries a reply subject. Failure handling is layered:

1. async in-memory retry (`rv-handler-retry-*` pool) — lost if the JVM dies; final failure replies `{status=ERROR}`;
2. `@RvCommand(persistent = true)` — message persisted to `rv_command_queue` **before** processing; on failure caller gets `{status=QUEUED}` and a background poller retries across restarts (at-least-once → handlers must be idempotent; retries never reply);
3. exhausted rows stay as `FAILED` dead letters, requeued manually via SQL.

Persistent chaining: `RvPersistentCommandQueue.submit(command, payload)` inserts a follow-up command directly (no RV round trip); with `@Transactional(rollbackFor = Exception.class)` on the handler, the business write and the submit commit atomically (`rollbackFor` is required because `TibrvException` is checked). The dispatcher invokes handlers through the Spring proxy, so `@Transactional` works.

Outbound path: `RendezvousPublisher` sends to named destinations configured under `aos.rendezvous.destinations.<name>` (one cached transport each), composing the outbound subject automatically. `request()` retries on timeout (same message resent → target handlers must be idempotent) and returns `null` only when every attempt timed out. `publish()` is fire-and-forget and does not fail when nobody listens.

Two consumption modes: default DQ (all members consume, load-balanced, `scheduler-weight` picks the scheduler) vs FT active/standby (`--aos.rendezvous.ft.enabled=true`; the `active-goal` highest-`ft.weight` members consume, others stand by and get promoted on failure). See the DQ-vs-FT comparison table in USAGE.md §6.

REST gateway: embedded Tomcat coexists with the RV subscriber, and the BOOT-role instance is the **single HTTP entry point for the whole service family** — other roles run headless (`--spring.main.web-application-type=none`), expose operations as `@RvCommand` handlers, and controllers in the application bridge to each role's destination (e.g. `SchApiController` → `sch` destination → the SCH role's `SCH_STATUS`). The shared bridge is `RvApiBridge`: one explicit endpoint per use case → RV command via `requestOnce` (single attempt, short `aos.api.rv-timeout`, never the retrying `request()` — Tomcat threads would starve) → reply status mapped to HTTP (`OK`→200, `QUEUED`→202, `NOT_FOUND`→404, `ERROR`→502, timeout→504). Generic `POST /rv/{command}` proxies are forbidden.

Adding a business module: create a library module depending on `aos-boot-core`, put `@RvCommand` handlers/controllers under `com.p2gether.aos.**` (component scan picks them up), add one dependency line to `aos-boot-application`. If the module belongs to one role only, gate its beans on the listener like `SchedulerConfiguration` does (`@ConditionalOnProperty` on `aos.rendezvous.subject.listener`).

The persistent queue runs on `spring.datasource` — local H2 file DB by default; `schema.sql` is H2 syntax (`GENERATED BY DEFAULT AS IDENTITY`, sample `MERGE INTO ... KEY`), so production DBs need adapted DDL and `spring.sql.init.mode` removed.
