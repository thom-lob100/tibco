# tibrvj — compile-only API stub

This is **not** the TIBCO Rendezvous library. It is a tiny standalone Maven project that produces an
artifact with the same coordinates as the real one — `com.tibco.tibrv:tibrvj:8.4.5` — but containing
**only the API signatures** that `aos-boot` compiles against (reliable messaging plus the
certified-messaging / distributed-queue classes: `TibrvCmTransport`, `TibrvCmQueueTransport`,
`TibrvCmListener`). Every method body throws `UnsupportedOperationException`; there is no Rendezvous
implementation and no TIBCO code here.

Originally copied from `smartaos-boot/ci/tibrv-stub`; this copy is a superset of that one, so
installing it keeps `smartaos-boot` compiling too.

## Why it exists

`tibrvj.jar` is proprietary and cannot be redistributed. This stub lets a machine without a TIBCO
installation verify that the Rendezvous layer still **compiles**. It cannot run Rendezvous messaging.

## How to install it

```bash
mvn -f tibrv-stub/pom.xml clean install   # publish the stub to the local repo
mvn -f aos-boot/pom.xml compile           # compile aos-boot against it
```

## Local development

Developers with a real TIBCO installation must install the **genuine** jar under the same coordinates,
which overrides this stub in their local repository:

```bash
mvn install:install-file -Dfile=/path/to/tibrvj.jar \
  -DgroupId=com.tibco.tibrv -DartifactId=tibrvj -Dversion=8.4.5 -Dpackaging=jar
```

This project is deliberately **outside the `aos-boot` reactor** (its `pom.xml` does not list it as a
module), so a normal `mvn package` never builds or installs the stub and cannot accidentally shadow
the real jar.

## Maintenance

If you add or change a TIBCO API call in `aos-boot`, mirror the signature in the matching
`src/main/java/com/tibco/tibrv/*.java` class here, or the build will fail to compile.
