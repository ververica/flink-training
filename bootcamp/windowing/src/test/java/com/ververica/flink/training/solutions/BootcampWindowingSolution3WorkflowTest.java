package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing3Workflow;

class BootcampWindowingSolution3WorkflowTest {

    @Test
    public void testBootcampWindowingSolution3Workflow() throws Exception {
        testWindowing3Workflow(new BootcampWindowingSolution3Workflow());
    }
}