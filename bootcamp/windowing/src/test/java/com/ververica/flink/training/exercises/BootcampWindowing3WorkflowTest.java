package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing3Workflow;

public class BootcampWindowing3WorkflowTest {

    @Test
    public void testBootcampWindowing3Workflow() throws Exception {
        testWindowing3Workflow(new BootcampWindowing3Workflow());
    }

}