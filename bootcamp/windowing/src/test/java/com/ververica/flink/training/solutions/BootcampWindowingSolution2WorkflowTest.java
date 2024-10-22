package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing2Workflow;

class BootcampWindowingSolution2WorkflowTest {

    @Test
    public void testBootcampWindowingSolution2Workflow() throws Exception {
        testWindowing2Workflow(new BootcampWindowingSolution2Workflow());
    }

}