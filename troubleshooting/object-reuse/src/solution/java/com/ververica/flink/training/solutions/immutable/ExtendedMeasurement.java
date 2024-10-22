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

/** Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement}. */
@TypeInfo(ExtendedMeasurement.ExtendedMeasurementTypeInfoFactory.class)
public class ExtendedMeasurement {

    private final Sensor sensor;
    private final Location location;
    private final MeasurementValue measurement;

    /** Constructor. */
    public ExtendedMeasurement(Sensor sensor, Location location, MeasurementValue measurement) {
        this.sensor = sensor;
        this.location = location;
        this.measurement = measurement;
    }

    public Sensor getSensor() {
        return sensor;
    }

    public Location getLocation() {
        return location;
    }

    public MeasurementValue getMeasurement() {
        return measurement;
    }

    public static class ExtendedMeasurementTypeInfoFactory
            extends TypeInfoFactory<ExtendedMeasurement> {
        @Override
        public TypeInformation<ExtendedMeasurement> createTypeInfo(
                Type t, Map<String, TypeInformation<?>> genericParameters) {
            return ExtendedMeasurementTypeInfo.INSTANCE;
        }
    }
}
