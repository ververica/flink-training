package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampWindowingWorkflowTestUtils.testWindowing2Workflow;

public class BootcampWindowing2WorkflowTest {

    @Test
    public void testBootcampWindowing2Workflow() throws Exception {
        testWindowing2Workflow(new BootcampWindowing2Workflow());
    }

}