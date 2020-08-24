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

# Lab: Advanced Tuning & Troubleshooting: Checkpointing

## Exercise

* Run [`CheckpointingJob`](src/main/java/com/ververica/flink/training/exercises/CheckpointingJob.java)
  - once with `FsStateBackend` (default mode if run locally)
  - once with `RocksDBStateBackend` (set your Flink cluster to use that; locally you can pass `--useRocksDB` to the job)
* Observe the job's healthiness.
* Fix the job so that checkpoints are not timing out (2 minutes)

**There are 2 ways of solving this exercise. Can you find both?**

-----

[**Back to Tuning & Troubleshooting Labs Overview**](../README.md)
