package com.ververica.flink.training.exercises;

import com.ververica.flink.training.solutions.BootcampEnrichmentSolution1Workflow;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingUSDEquivalentWorkflow;

class BootcampEnrichment1WorkflowTest {

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        testAddingUSDEquivalentWorkflow(new BootcampEnrichment1Workflow());
    }
}