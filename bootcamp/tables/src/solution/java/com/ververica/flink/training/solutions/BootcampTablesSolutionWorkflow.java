package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampTablesWorkflow;
import org.apache.flink.table.api.*;
import org.apache.flink.util.Preconditions;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Use the Table API to group by country and count transactions per minute
 */
public class BootcampTablesSolutionWorkflow extends BootcampTablesWorkflow {

    public BootcampTablesSolutionWorkflow(TableEnvironment tEnv) {
        super(tEnv);
    }

    @Override
    public Table build() {
        Schema schema = Schema.newBuilder()
                .column("transactionId", DataTypes.BIGINT())
                .column("customerId", DataTypes.BIGINT())
                .column("transactionCompleted", DataTypes.BOOLEAN())
                .column("transactionTime", DataTypes.BIGINT())
                .build();

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

        tEnv.createTemporaryTable("carts", tableDescriptor);
        Table cartsTable = tEnv.from("carts");

        Table resultTable = cartsTable
                .groupBy($("customerId"))
                .select(
                        $("customerId"),
                        $("customerId").count().as("transaction_count")
                );

        return resultTable;
    }

}
