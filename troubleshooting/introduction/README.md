<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Lab: Introduction to Tuning & Troubleshooting

## Introduction

This lab provides the basis of the hands-on part of the "Introduction to Apache Flink Tuning & Troubleshooting"
training by Ververica. Please follow the [Setup Instructions](../../README.md#setup-your-development-environment) first
and then continue reading here.

### Infrastructure

During the training, participants will be asked to run the Flink job `TroubledStreamingJob` locally as well as on
Ververica Platform.

### Running Locally

Executing `com.ververica.flink.training.exercises.TroubledStreamingJob#main()` should create a local Flink cluster
running the troubled streaming job and serving Flink's Web UI at http://localhost:8081.
If port 8081 is blocked and Flink won't start, or if the Web UI is not showing up, you can also configure and force
the local mode via the `--local` program argument and set the port the Web UI is listening on:

* `--local -1`: defaults via `StreamExecutionEnvironment.getExecutionEnvironment()` (no Web UI, no extra settings)
* `--local <port|port-range>`: uses `StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()` and
  - sets a `fixedDelayRestart` failure strategy with 15s delay and infinite restarts
  - configures the Web UI to listen on a port in the given range
    If more than one port is given, please find the port in the logs from this line:
```
12:11:04.062 [main] INFO o.a.f.r.d.DispatcherRestEndpoint - Web frontend listening at http://localhost:8081.
```

You can also specify the parallelism via `--parallelism <number>` if needed (may be valuable in local setups).

### The Flink Job

This simple Flink job reads measurement data from a Kafka topic with eight partitions. For the purpose of this training,
the `KafkaConsumer` is replaced by `FakeKafkaSource`. The result of a calculation based on the measurement value is
averaged over 1 second. The overall flow is depicted below:

```
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
|                   |     |                       |     |                 |     |                      |     |                    |
| Fake Kafka Source | --> | Watermarks/Timestamps | --> | Deserialization | --> | Windowed Aggregation | --> | Sink: NormalOutput |
|                   |     |                       |     |                 |     |                      |     |                    |
+-------------------+     +-----------------------+     +-----------------+     +----------------------+     +--------------------+
                                                                                            \
                                                                                             \               +--------------------+
                                                                                              \              |                    |
                                                                                               +-----------> | Sink: LateDataSink |
                                                                                                             |                    |
                                                                                                             +--------------------+
```

In local mode, the sinks print their values on `stdout` (NormalOutput) and `stderr` (LateDataSink), which simplifies debugging.
Otherwise, a `DiscardingSink` is used for each sink.

-----

[**Back to Tuning & Troubleshooting Labs Overview**](../README.md)
