# Lab: Advanced Tuning & Troubleshooting: RocksDB

## Exercise

For each of the following scenarios:

* Run the scenario with the settings provided in your Ververica Platform instance.
* Observe the job and identify bottlenecks.
* Tune RocksDB through [configuration options provided by Flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/config/#advanced-rocksdb-state-backends-options).
* (Expert) Tune RocksDB further via a custom [`RocksDBOptionsFactory`](https://github.com/apache/flink/blob/release-1.14.0/flink-state-backends/flink-statebackend-rocksdb/src/main/java/org/apache/flink/contrib/streaming/state/RocksDBOptionsFactory.java) implementation and enable this via [`state.backend.rocksdb.options-factory`](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/config/#state-backend-rocksdb-options-factory)

### Scenarios

1. [`RocksDBTuningJob1`](src/provided/java/com/ververica/flink/training/provided/RocksDBTuningJob1.java)
2. [`RocksDBTuningJob2`](src/provided/java/com/ververica/flink/training/provided/RocksDBTuningJob2.java)

> **:heavy_exclamation_mark: Important:** Do not change the code of these two classes (or anything marked with `@DoNotChangeThis` in general)! This exercise should be solved by tuning RocksDB only.

-----

[**Back to Tuning & Troubleshooting Labs Overview**](../README.md)
