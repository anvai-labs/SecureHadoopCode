# Secure Hadoop Code

This repository contains examples for secured Hadoop environments. It is being modernized in
isolated modules because the original reactor targets obsolete CDH, HBase, Kafka, Hive, and Oozie
versions.

SecureMapReduce is the first supported module. It targets Java 17 with two explicit dependency
profiles and is covered by unit tests plus JaCoCo line and branch coverage floors:

* `apache-upstream` (default): Apache Hadoop 3.5.0; tested with Java 17 and 21.
* `cdp-7.3.2-sp1`: Cloudera Runtime 7.3.2 SP1 Hadoop
  `3.4.2.7.3.2.10000-317`; tested with Java 17.

Cluster-provided Hadoop dependencies use Maven's `provided` scope so application artifacts do not
bundle a second runtime. Build either compatibility target with:

```bash
./mvnw -B -ntp -Papache-upstream -pl SecureMapReduce -am clean verify
./mvnw -B -ntp -Pcdp-7.3.2-sp1 -pl SecureMapReduce -am clean verify
```

The enforced minimums are 80% line coverage and 70% branch coverage. The current suite reports
92.9% line coverage and 87.5% branch coverage for the supported module; CI recalculates these
metrics for every change rather than trusting the documented snapshot.

The remaining modules are legacy-only and must not be modified until their dependencies, security
configuration, tests, and CI gates are modernized. CI enforces that boundary rather than presenting
the untested legacy reactor as supported.
