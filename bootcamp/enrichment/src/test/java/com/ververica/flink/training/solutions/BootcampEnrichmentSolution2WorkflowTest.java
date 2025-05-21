package com.ververica.flink.training.solutions;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampEnrichmentWorkflowTestUtils.testAddingProductWeightWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampEnrichmentSolution2WorkflowTest {

    @Test
    // FIXME - reenable when intermittent failure is resolved
    @Disabled
    public void testAddingProductWeight() throws Exception {
        testAddingProductWeightWorkflow(new BootcampEnrichmentSolution2Workflow());
    }
}