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

public class MeasurementValueSerializer extends TypeSerializerSingleton<MeasurementValue> {

    private MeasurementValueSerializer() {}

    static final MeasurementValueSerializer INSTANCE = new MeasurementValueSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public MeasurementValue createInstance() {
        return null;
    }

    @Override
    public MeasurementValue copy(MeasurementValue from) {
        return new MeasurementValue(from.getValue(), from.getAccuracy(), from.getTimestamp());
    }

    @Override
    public MeasurementValue copy(MeasurementValue from, MeasurementValue reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return Double.BYTES + Float.BYTES + Long.BYTES;
    }

    @Override
    public void serialize(MeasurementValue record, DataOutputView target) throws IOException {
        target.writeDouble(record.getValue());
        target.writeFloat(record.getAccuracy());
        target.writeLong(record.getTimestamp());
    }

    @Override
    public MeasurementValue deserialize(DataInputView source) throws IOException {
        double value = source.readDouble();
        float accuracy = source.readFloat();
        long timestamp = source.readLong();
        return new MeasurementValue(value, accuracy, timestamp);
    }

    @Override
    public MeasurementValue deserialize(MeasurementValue reuse, DataInputView source)
            throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeDouble(source.readDouble());
        target.writeFloat(source.readFloat());
        target.writeLong(source.readLong());
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<MeasurementValue> snapshotConfiguration() {
        return new MeasurementValueSerializerSnapshot();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class MeasurementValueSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<MeasurementValue> {

        /** Returns a snapshot pointing to the singleton serializer instance. */
        public MeasurementValueSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
