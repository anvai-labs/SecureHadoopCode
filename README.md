# Secure Hadoop Code

This repository contains examples for secured Hadoop environments. It is being modernized in
isolated modules because the original reactor targets obsolete CDH, HBase, Kafka, Hive, and Oozie
versions.

SecureMapReduce, HBaseBulkLoad, and QueryKerberosAuthHS2 are supported modules. They target Java 17
with two explicit dependency profiles and are covered by unit tests plus JaCoCo line and branch
coverage floors:

* `apache-upstream` (default): Apache Hadoop 3.5.0; tested with Java 17 and 21.
* `cdp-7.3.2-sp1`: Cloudera Runtime 7.3.2 SP1 Hadoop
  `3.4.2.7.3.2.10000-317`; tested with Java 17.

Cluster-provided Hadoop dependencies use Maven's `provided` scope so application artifacts do not
bundle a second runtime. Build either compatibility target with:

```bash
./mvnw -B -ntp -Papache-upstream -pl SecureMapReduce -am clean verify
./mvnw -B -ntp -Pcdp-7.3.2-sp1 -pl SecureMapReduce -am clean verify
./mvnw -B -ntp -Papache-upstream -pl HBaseBulkLoad,SecureHiveServer2Query/QueryKerberosAuthHS2 -am clean verify
./mvnw -B -ntp -Pcdp-7.3.2-sp1 -pl HBaseBulkLoad,SecureHiveServer2Query/QueryKerberosAuthHS2 -am clean verify
```

SecureMapReduce enforces 80% line and 70% branch coverage. HBaseBulkLoad enforces 70% line and 75%
branch coverage; its unexecuted lines are the real-cluster adapter, while its argument validation,
configuration, row parsing, UTF-8 conversion, and safe orchestration boundary are unit-tested.
QueryKerberosAuthHS2 enforces 90% line and 75% branch coverage. CI recalculates these metrics for
every change rather than trusting a documented snapshot.

HBaseBulkLoad never recursively deletes its output path; callers must supply a new destination. It
uses HBase's supported `BulkLoadHFiles` interface and the runtime-provided client. The Hive example
accepts connection parameters instead of embedding hosts, principals, keytabs, truststore paths, or
passwords. Configure TLS trust through the JVM truststore or your deployment's secret manager; do
not append credentials to the JDBC URL.

Kerberos remains the native authentication mechanism for these Hadoop client examples. For modern
browser/OIDC SSO, put Apache Knox or another identity-aware gateway in front of the cluster and use
short-lived bearer tokens at that boundary; do not reinterpret an OIDC token as a Kerberos keytab.

SecureKafkaDev, OozieJavaRestExample, and QueryKerberosAuthLDAPS remain legacy-only and must not be
modified until their dependencies, security configuration, tests, and CI gates are modernized. CI
enforces that boundary rather than presenting the untested legacy reactor as supported.
