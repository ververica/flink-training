package com.ververica.flink.training.solutions;

import com.ververica.flink.training.exercises.BootcampFailuresWorkflowTest;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampFailuresWorkflowTestUtils.testWorkflow;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class BootcampFailuresSolutionWorkflowTest extends BootcampFailuresWorkflowTest {

    @Test
    public void testGettingCorrectResultsAfterFailure() throws Exception {
        testWorkflow(new BootcampFailuresSolution1Config(), true);
    }

    @Test
    public void testLatency() throws Exception {
        testWorkflow(new BootcampFailuresSolution1Config(), false);
    }

    @Test
    public void testLowerLatency() throws Exception {
        testWorkflow(new BootcampFailuresSolution2Config(), false);
    }

}