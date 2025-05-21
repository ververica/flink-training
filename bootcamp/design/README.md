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

# Lab: eCommerce Workflow Design

## Exercise 1

You'll start with the [BootcampDesignMamboWorkflow](src/provided/java/com/ververica/flink/training/provided/BootcampDesignMamboWorkflow.java),
which is an example of a workflow that's gotten a bit too big for its own good. This workflow
consumes a stream of ShoppingCartRecords, and generates two results. One is analytics,
calculating the number of unique transactions per hour per customer. The second
result is a "bogus shopper" detector, which calculates the number of abandoned
products per customer per minute, and returns any that exceed a configurable limit.

You need to split it into two separate workflows, one called `BootcampDesignAnalyticsWorkflow`,
and the other called `BootcampDesignDetectionWorkflow` The first workflow generates
both analytics results and abandoned cart item results.

The second workflow takes as input the abandoned cart items, and generates
bad customer results, using the logic already found in the mambo workflow.

So you'll need to split the manbo workflow into two pieces, where the first workflow
generates multiple results (both analytics and input to the detection workflow).

To test, run the `testBridgingWorkflows` test in [BootcampDesignTest](src/test/java/com/ververica/flink/training/exercises/BootcampDesignTest.java)

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
