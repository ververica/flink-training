
# Lab: eCommerce Tables

## Introduction

This workflow uses the Flink Table API to create a stream of fake records for a transactions that
it then aggregates.

## Exercise 1

You'll need to modify the
[BootcampTablesWorkflow](src/main/java/com/ververica/flink/training/exercises/BootcampTablesWorkflow.java)
code to use the Table APIs to define the fake table source (using the Flink "datagen" connector), then
group by the customerId and count the number of transaction.

To test, you should run the `BootcampTablesWorkflowTest.testFlinkSQL()` test. This doesn't do any validation
per-se, but when you've properly implemented the `BootcampTablesWorkflow`, it will print out results that 
should look something like:

```sql
+----+----------------------+----------------------+
| op |           customerId |    transaction_count |
+----+----------------------+----------------------+
| +I |                 1002 |                    1 |
| -U |                 1002 |                    1 |
| +U |                 1002 |                    2 |
| -U |                 1002 |                    2 |
| +U |                 1002 |                    3 |
| -U |                 1002 |                    3 |
| +U |                 1002 |                    4 |
| -U |                 1002 |                    4 |
| +U |                 1002 |                    5 |
| -U |                 1002 |                    5 |
| +U |                 1002 |                    6 |
| -U |                 1002 |                    6 |
| +U |                 1002 |                    7 |
| -U |                 1002 |                    7 |
| +U |                 1002 |                    8 |
| -U |                 1002 |                    8 |
| +U |                 1002 |                    9 |
| -U |                 1002 |                    9 |
| +U |                 1002 |                   10 |
| -U |                 1002 |                   10 |
```

These are rows that have, in the first column, the RowKind (insert, retraction as -U/+U), followed by the
`customerId` and a `transaction_count`. Note that the count increases steadily, as this is a dynamic table
that is continuously being updated.

### Hints

1. Use the `SchemaBuilder.column(name, type)` to add columns to the schema.
1. Use the `TableDescrptor.option(name, value)` to add configuration settings for the datagen connector.
2. Use the `Expressions.$("field name") to specify a field being used in a Table API call`
3. Use the `$("field name").count()` to get the count for a group, when selecting output fields.

-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
