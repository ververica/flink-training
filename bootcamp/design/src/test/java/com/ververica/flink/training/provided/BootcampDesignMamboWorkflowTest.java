/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Test;

import static com.ververica.flink.training.provided.BootcampDesignWorkflowTestUtils.*;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.ArrayList;
import java.util.List;

/*
 * Solution to the workflow design exercise. We
 */
public class BootcampDesignMamboWorkflowTest {

    @Test
    public void testMamboWorkflow() throws Exception {
        final StreamExecutionEnvironment env = FlinkClusterUtils.createConfiguredTestEnvironment(2);

        List<ShoppingCartRecord> records = makeCartRecords();

        AnalyticsSink analyticsSink = new AnalyticsSink();
        BadCustomerSink badCustomerSink = new BadCustomerSink();

        new BootcampDesignMamboWorkflow()
                .setCartStream(env.fromData(records).setParallelism(1))
                .setAnalyticsSink(analyticsSink)
                .setBadCustomerSink(badCustomerSink)
                .setMaxAbandonedPerMinute(1)
                .build();

        env.execute("BootcampDesignMamboWorkflow");

        validateAnalyticsResults(analyticsSink.getSink());
        validateBadCustomerResults(badCustomerSink.getSink());
    }


}