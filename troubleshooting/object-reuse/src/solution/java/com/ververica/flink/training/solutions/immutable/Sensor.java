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
import java.util.Objects;

/**
 * Immutable variant of {@link com.ververica.flink.training.provided.ExtendedMeasurement.Sensor}.
 */
@SuppressWarnings("WeakerAccess")
@TypeInfo(Sensor.SensorTypeInfoFactory.class)
public class Sensor {
    public enum SensorType {
        Temperature,
        Wind
    }

    private final long sensorId;
    private final long vendorId;
    private final SensorType sensorType;

    /** Constructor. */
    public Sensor(long sensorId, long vendorId, SensorType sensorType) {
        this.sensorId = sensorId;
        this.vendorId = vendorId;
        this.sensorType = sensorType;
    }

    public long getSensorId() {
        return sensorId;
    }

    public long getVendorId() {
        return vendorId;
    }

    public SensorType getSensorType() {
        return sensorType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Sensor sensor = (Sensor) o;
        return sensorId == sensor.sensorId
                && vendorId == sensor.vendorId
                && sensorType == sensor.sensorType;
    }

    @Override
    public int hashCode() {
        // NOTE: do not use the enum directly here. Why?
        // -> try with Sensor as a key in a distributed setting and see for yourself!
        return Objects.hash(sensorId, vendorId, sensorType.ordinal());
    }

    public static class SensorTypeInfoFactory extends TypeInfoFactory<Sensor> {
        @Override
        public TypeInformation<Sensor> createTypeInfo(
                Type t, Map<String, TypeInformation<?>> genericParameters) {
            return SensorTypeInfo.INSTANCE;
        }
    }
}
