package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.*;

class BootcampEnrichmentSolution1WorkflowTest {

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        testAddingUSDEquivalentWorkflow(new BootcampEnrichmentSolution1Workflow());
    }
}