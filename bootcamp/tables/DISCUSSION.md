# Lab: Bootcamp Tables (Discussion)

## Exercise 1 Solution

See the [README](README.md#exercise-1) file for the steps.

To define the table schema...

```java
        Schema schema = Schema.newBuilder()
                .column("transactionId", DataTypes.BIGINT())
                .column("customerId", DataTypes.BIGINT())
                .column("transactionCompleted", DataTypes.BOOLEAN())
                .column("transactionTime", DataTypes.BIGINT())
                .build();
```

To create the table descriptor, we'll set a bunch of options for the `datagen`
connector, then specify the schema via `.schema(schema)`, and build it. In order
to set reasonable transaction times, we'll set the min/max based on the current
system time.

```java
        long startTime = System.currentTimeMillis();
        TableDescriptor tableDescriptor = TableDescriptor.forConnector("datagen")
                .option("number-of-rows", "100")
                .option("fields.transactionId.min", "100000")
                .option("fields.transactionId.max", "110000")
                .option("fields.customerId.min", "1000")
                .option("fields.customerId.max", "1005")
                .option("fields.transactionTime.min", Long.toString(startTime))
                .option("fields.transactionTime.max", Long.toString(startTime + Duration.ofMinutes(1).toMillis()))
                .schema(schema)
                .build();
```

In order to define a table based on the descriptor, we need to create a temporary table that
gives it a name in the Table execution environment. Once we have this, we can create
a Table object that we'll need to actually perform Table API operations.

```java
        tEnv.createTemporaryTable("carts", tableDescriptor);
        Table cartsTable = tEnv.from("carts");=
```

Finally, we can use a `groupBy`, followed by a `select`, where we use the
grouping field's `.count()` to get the count for each group, which we then
rename as the `transaction_count`.

```java
        Table resultTable = cartsTable
                .groupBy($("customerId"))
                .select(
                        $("customerId"),
                        $("customerId").count().as("transaction_count")
                );

```
-----

[**Back to Bootcamp Overview**](../../README-Bootcamp.md)
