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

# Lab: Advanced Tuning & Troubleshooting: Throughput

## Exercise

* Improve the throughput of [`ThroughputJob`](src/main/java/com/ververica/flink/training/exercises/ThroughputJob.java).
  - Identify where backpressure originates.
  - You can also use a (local) profiler of your choice for identifying further optimisation potential.
    - Disable the local print sink for this (why?)

## Stats from Throughput Task row in Grafana

- [ThroughputJob](src/main/java/com/ververica/flink/training/exercises/ThroughputJob.java): ~160 K ops
- [ThroughputJobSolution1](src/solution/java/com/ververica/flink/training/solutions/ThroughputJobSolution1.java): ~215 K ops
- [ThroughputJobSolution2](src/solution/java/com/ververica/flink/training/solutions/ThroughputJobSolution2.java): ~930 K ops

-----

[**Back to Tuning & Troubleshooting Labs Overview**](../README.md)
