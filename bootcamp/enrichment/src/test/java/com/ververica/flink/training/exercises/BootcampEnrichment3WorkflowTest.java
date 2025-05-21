package com.ververica.flink.training.exercises;

import com.ververica.flink.training.solutions.BootcampEnrichmentSolution3Workflow;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingUSDEquivalentWorkflow;

class BootcampEnrichment3WorkflowTest {

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        testAddingUSDEquivalentWorkflow(new BootcampEnrichmentSolution3Workflow());
    }
}