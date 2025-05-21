package com.ververica.flink.training.exercises;

import com.ververica.flink.training.solutions.BootcampTablesSolutionWorkflow;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.Test;

class BootcampTablesWorkflowTest {

    @Test
    public void testFlinkSQL() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table result = new BootcampTablesWorkflow(tEnv).build();
        result.execute().print();
    }

}