package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingUSDEquivalentWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampEnrichmentSolution3WorkflowTest {

    @Test
    public void testAddingUSDEquivalent() throws Exception {
        testAddingUSDEquivalentWorkflow(new BootcampEnrichmentSolution3Workflow());
    }
}