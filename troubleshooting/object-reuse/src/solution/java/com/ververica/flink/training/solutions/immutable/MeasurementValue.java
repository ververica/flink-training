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

package com.ververica.flink.training.solutions.immutable;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.lang.reflect.Type;
import java.util.Map;

/**
 * Immutable variant of {@link
 * com.ververica.flink.training.provided.ExtendedMeasurement.MeasurementValue}.
 */
@TypeInfo(MeasurementValue.MeasurementValueTypeInfoFactory.class)
public class MeasurementValue {
    private final double value;
    private final float accuracy;
    private final long timestamp;

    /** Constructor. */
    public MeasurementValue(double value, float accuracy, long timestamp) {
        this.value = value;
        this.accuracy = accuracy;
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public float getAccuracy() {
        return accuracy;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public static class MeasurementValueTypeInfoFactory extends TypeInfoFactory<MeasurementValue> {
        @Override
        public TypeInformation<MeasurementValue> createTypeInfo(
                Type t, Map<String, TypeInformation<?>> genericParameters) {
            return MeasurementValueTypeInfo.INSTANCE;
        }
    }
}
