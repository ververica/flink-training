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

package com.ververica.flink.training.exercises;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.FakeKafkaRecord;
import com.ververica.flink.training.common.Measurement;

import java.io.IOException;

/**
 * Deserializes {@link FakeKafkaRecord} into {@link Measurement} objects, ignoring deserialization
 * failures.
 */
@DoNotChangeThis
public class MeasurementDeserializer extends RichFlatMapFunction<FakeKafkaRecord, Measurement> {

    private static final long serialVersionUID = -5805258552949837150L;

    private transient ObjectMapper mapper;

    @Override
    public void open(final Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    private Measurement deserialize(final byte[] bytes) throws IOException {
        return mapper.readValue(bytes, Measurement.class);
    }

    @Override
    public void flatMap(final FakeKafkaRecord kafkaRecord, final Collector<Measurement> out) {
        try {
            out.collect(deserialize(kafkaRecord.getValue()));
        } catch (IOException ignored) {
        }
    }
}
