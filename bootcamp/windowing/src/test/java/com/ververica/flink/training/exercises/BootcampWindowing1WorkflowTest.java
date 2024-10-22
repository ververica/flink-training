package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing1Workflow;

public class BootcampWindowing1WorkflowTest {

    @Test
    public void testBootcampWindowing1Workflow() throws Exception {
        testWindowing1Workflow(new BootcampWindowing1Workflow());
    }

}