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

# Lab: eCommerce Enrichment

## Introduction

The goal of this lab is to use two different techniques to enrich a stream
of `ShoppingCartRecord` records with additional information.

## Exercise 1

Take a look at the [BootcampEnrichment1Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampEnrichment1Workflow.java)
class to see how the rather simple workflow uses a custom map function called
[CalcTotalUSDollarPriceFunction](src/main/java/com/ververica/flink/training/exercises/CalcTotalUSDollarPriceFunction.java)
to calculate the total value of all items in the shopping cart, normalized to
US dollars.

In order to do this, it makes use of a "Currency Exchange Rate" API
that is provided by the [CurrencyRateAPI](../common/src/main/java/com/ververica/flink/training/common/CurrencyRateAPI.java)
class. This simulates an external service, one that would typically be called via
HTTP requests.

Modify the `CalcTotalUSDollarPriceFunction` to use this API to perform the
API request and calculation. You can test this via either running the
`BootcampEnrichment1WorkflowTest.testAddingUSDEquivalent()` test (integration)
or the `CalcTotalUSDollarPriceSolutionFunctionTest` (function test). Take
a look at how the function test leverages Flink's test harness support.

## Exercise 2

Take a look at the [BootcampEnrichment2Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampEnrichment2Workflow.java)
class to see how the workflow uses a custom flatMap function called
[ExplodeShoppingCartFunction](src/main/java/com/ververica/flink/training/exercises/ExplodeShoppingCartFunction.java)
to convert single `ShoppingCartRecord` records into some number of
`ProductRecord` records, and then connects this stream of product records with a stream
of product information records, and uses the
[AddProductInfoFunction](src/main/java/com/ververica/flink/training/exercises/AddProductInfoFunction.java)
to join the two stream.

You'll need to fill out the `ExplodeShoppingCartFunction` and `AddProductInfoFunction` classes
to get the `BootcampEnrichment2WorkflowTest.testAddingProductWeight` integration test to pass.

## Exercise 3

Take a look at the [BootcampEnrichment3Workflow](src/main/java/com/ververica/flink/training/exercises/BootcampEnrichment3Workflow.java)
class. This is exactly the same as the previous `BootcampEnrichment1Workflow`, except that
it uses a new custom map function called
[CalcTotalUSDollarPriceWithCacheFunction](src/main/java/com/ververica/flink/training/exercises/CalcTotalUSDollarPriceWithCacheFunction.java)
when calculating the total value of all items in the shopping cart, normalized to
US dollars.

This new function is the same as the old `CalcTotalUSDollarPriceFunction`, except
that it (should) use a cache to avoid unnecessary look-ups. Modify the
new version to support caching.

You can test this via running the
`BootcampEnrichment3WorkflowTest.testAddingUSDEquivalent()` test (integration).

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
