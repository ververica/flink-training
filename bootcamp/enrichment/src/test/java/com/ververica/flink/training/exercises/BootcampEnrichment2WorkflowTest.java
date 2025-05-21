package com.ververica.flink.training.exercises;

import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingProductWeightWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampEnrichment2WorkflowTest {

    @Test
    public void testAddingProductWeight() throws Exception {
        testAddingProductWeightWorkflow(new BootcampEnrichment2Workflow());
    }
}