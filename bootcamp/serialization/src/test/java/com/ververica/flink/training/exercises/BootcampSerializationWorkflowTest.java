package com.ververica.flink.training.exercises;

import com.ververica.flink.training.common.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.ververica.flink.training.provided.BootcampSerializationWorkflowTestUtils.testWorkflow;

public class BootcampSerializationWorkflowTest {

    @Test
    public void testBootcampSerializationWorkflow() throws Exception {
        testWorkflow(new BootcampSerializationWorkflow());
    }

}