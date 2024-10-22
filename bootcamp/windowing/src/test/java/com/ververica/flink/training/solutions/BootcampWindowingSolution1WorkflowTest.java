package com.ververica.flink.training.solutions;

import com.ververica.flink.training.common.*;
import com.ververica.flink.training.exercises.BootcampWindowing1WorkflowTest;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing1Workflow;


class BootcampWindowingSolution1WorkflowTest {

    @Test
    public void testBootcampWindowingSolution1Workflow() throws Exception {
        testWindowing1Workflow(new BootcampWindowingSolution1Workflow());
    }
}