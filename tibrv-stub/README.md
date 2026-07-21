# tibrvnative — development API stub

This is **not** the TIBCO Rendezvous library. It is a tiny standalone Maven project that produces an
artifact with the same coordinates as the company JNI implementation —
`com.tibco.tibrv:tibrvnative:8.4.5` — but containing API signatures and a JVM-local test bus
that `aos-boot` compiles and locally exercises against (reliable messaging plus the
certified-messaging / distributed-queue classes: `TibrvCmTransport`, `TibrvCmQueueTransport`,
`TibrvCmListener`). It contains no TIBCO code and cannot communicate with a real `rvd` or another JVM.

Originally copied from `smartaos-boot/ci/tibrv-stub`; this copy is a superset of that one, so
installing it keeps `smartaos-boot` compiling too.

## Why it exists

`tibrvnative.jar` is proprietary and cannot be redistributed. This stub lets a machine without a TIBCO
installation compile and test application logic in one JVM. It is never a native RV implementation.

## How to install it

```bash
mvn -f tibrv-stub/pom.xml clean install   # publish the stub to the local repo
mvn -f aos-boot/pom.xml compile           # compile aos-boot against it
```

## Local development

Developers with a real TIBCO installation must install the **genuine** jar under the same coordinates,
which overrides this stub in their local repository:

```bash
mvn install:install-file -Dfile=/path/to/tibrvnative.jar \
  -DgroupId=com.tibco.tibrv -DartifactId=tibrvnative -Dversion=<COMPANY_RV_VERSION> -Dpackaging=jar
```

This project is deliberately **outside the `aos-boot` reactor** (its `pom.xml` does not list it as a
module), so a normal `mvn package` never builds or installs the stub and cannot accidentally shadow
the real jar.

## Maintenance

If you add or change a TIBCO API call in `aos-boot`, mirror the signature in the matching
`src/main/java/com/tibco/tibrv/*.java` class here, or the build will fail to compile.
