package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampSerializationWorkflowTestUtils.testWorkflow;

class BootcampSerializationSolutionWorkflowTest {

    @Test
    public void testBootcampSerializationSolutionWorkflow() throws Exception {
        testWorkflow(new BootcampSerializationSolutionWorkflow());
    }
}
