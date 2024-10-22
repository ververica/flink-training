package com.ververica.flink.training.exercises;

import org.apache.flink.table.api.*;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * Use the Table API to group by customerId and count records
 */
public class BootcampTablesWorkflow {

    protected TableEnvironment tEnv;

    public BootcampTablesWorkflow(TableEnvironment tEnv) {
        Preconditions.checkNotNull(tEnv, "env must not be null");
        this.tEnv = tEnv;
    }

    public Table build() {
        Schema schema = Schema.newBuilder()
                // TODO - define four columns, for transactionId, customerId,
                // transactionCompleted, and transactionTime. All are longs
                // aka "BIGINT" other than transactionCompleted, which is a boolean.
                .build();


        long startTime = System.currentTimeMillis();
        TableDescriptor tableDescriptor = TableDescriptor.forConnector("datagen")
                // TODO - configure the datagen connector to return 100 rows,
                // where the transactionId is between 100000 and 110000, the
                // customerId is between 1000 and 1005, and the transactionTime
                // is between the current time and now + 1 minute.
                .schema(schema)
                .build();

        // TODO - use the tEnv to create a temporary table called "carts", which
        // we can then use in SQL statements.

        // TODO - Create a Table from the "carts" table, group by customerId, then select
        // the customerId and the count per customerId, which should be named
        // "transaction_count". return this as the result of the build() call.

        return null;
    }

}
