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

package org.apache.flink.training.exercises.hourlytips;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.training.exercises.common.datatypes.TaxiFare;
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator;
import org.apache.flink.training.exercises.testing.ComposedPipeline;
import org.apache.flink.training.exercises.testing.ExecutablePipeline;
import org.apache.flink.training.exercises.testing.TestSink;
import org.apache.flink.training.solutions.hourlytips.HourlyTipsTableSolution;

import org.junit.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class HourlyTipsTableTest extends HourlyTipsTest {

    @Test
    public void testWithDataGenerator() throws Exception {

        // the TaxiFareGenerator is deterministic, and will produce these results if the
        // watermarking doesn't produce late events
        TaxiFareGenerator source = TaxiFareGenerator.runFor(Duration.ofMinutes(180));
        Tuple3<Long, Long, Float> hour1 = Tuple3.of(t(60).toEpochMilli(), 2013000089L, 76.0F);
        Tuple3<Long, Long, Float> hour2 = Tuple3.of(t(120).toEpochMilli(), 2013000197L, 71.0F);
        Tuple3<Long, Long, Float> hour3 = Tuple3.of(t(180).toEpochMilli(), 2013000118L, 83.0F);

        assertThat(results(source)).containsExactlyInAnyOrder(hour1, hour2, hour3);
    }

    private static final ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> exercise =
            (source, sink) -> new HourlyTipsTableExercise(source, sink).execute();

    private static final ExecutablePipeline<TaxiFare, Tuple3<Long, Long, Float>> solution =
            (source, sink) -> new HourlyTipsTableSolution(source, sink).execute();

    private static final ComposedPipeline<TaxiFare, Tuple3<Long, Long, Float>> testPipeline =
            new ComposedPipeline<>(exercise, solution);

    protected List<Tuple3<Long, Long, Float>> results(SourceFunction<TaxiFare> source)
            throws Exception {

        TestSink<Tuple3<Long, Long, Float>> sink = new TestSink<>();
        JobExecutionResult jobResult = testPipeline.execute(source, sink);
        return sink.getResults(jobResult);
    }
}
