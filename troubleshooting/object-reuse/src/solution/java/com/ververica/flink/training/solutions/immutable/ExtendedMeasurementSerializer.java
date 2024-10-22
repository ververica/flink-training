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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class ExtendedMeasurementSerializer extends TypeSerializerSingleton<ExtendedMeasurement> {

    private ExtendedMeasurementSerializer() {}

    static final ExtendedMeasurementSerializer INSTANCE = new ExtendedMeasurementSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public ExtendedMeasurement createInstance() {
        return null;
    }

    @Override
    public ExtendedMeasurement copy(ExtendedMeasurement from) {
        return new ExtendedMeasurement(
                SensorSerializer.INSTANCE.copy(from.getSensor()),
                LocationSerializer.INSTANCE.copy(from.getLocation()),
                MeasurementValueSerializer.INSTANCE.copy(from.getMeasurement()));
    }

    @Override
    public ExtendedMeasurement copy(ExtendedMeasurement from, ExtendedMeasurement reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return SensorSerializer.INSTANCE.getLength()
                + LocationSerializer.INSTANCE.getLength()
                + MeasurementValueSerializer.INSTANCE.getLength();
    }

    @Override
    public void serialize(ExtendedMeasurement record, DataOutputView target) throws IOException {
        SensorSerializer.INSTANCE.serialize(record.getSensor(), target);
        LocationSerializer.INSTANCE.serialize(record.getLocation(), target);
        MeasurementValueSerializer.INSTANCE.serialize(record.getMeasurement(), target);
    }

    @Override
    public ExtendedMeasurement deserialize(DataInputView source) throws IOException {
        Sensor sensor = SensorSerializer.INSTANCE.deserialize(source);
        Location location = LocationSerializer.INSTANCE.deserialize(source);
        MeasurementValue measurement = MeasurementValueSerializer.INSTANCE.deserialize(source);
        return new ExtendedMeasurement(sensor, location, measurement);
    }

    @Override
    public ExtendedMeasurement deserialize(ExtendedMeasurement reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        SensorSerializer.INSTANCE.copy(source, target);
        LocationSerializer.INSTANCE.copy(source, target);
        MeasurementValueSerializer.INSTANCE.copy(source, target);
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<ExtendedMeasurement> snapshotConfiguration() {
        return new ExtendedMeasurementSerializerSnapshot();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class ExtendedMeasurementSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<ExtendedMeasurement> {

        /** Returns a snapshot pointing to the singleton serializer instance. */
        public ExtendedMeasurementSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
