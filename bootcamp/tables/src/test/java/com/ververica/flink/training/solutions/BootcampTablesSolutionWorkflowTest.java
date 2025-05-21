package com.ververica.flink.training.solutions;

import org.apache.flink.table.api.*;
import org.junit.jupiter.api.Test;

class BootcampTablesSolutionWorkflowTest {

    @Test
    public void testFlinkSQL() throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        Table result = new BootcampTablesSolutionWorkflow(tEnv).build();
        result.execute().print();

    }

}