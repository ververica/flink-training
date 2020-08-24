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

public class SensorSerializer extends TypeSerializerSingleton<Sensor> {
    private static final Sensor.SensorType[] SENSOR_TYPES = Sensor.SensorType.values();

    private SensorSerializer() {}

    static final SensorSerializer INSTANCE = new SensorSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Sensor createInstance() {
        return null;
    }

    @Override
    public Sensor copy(Sensor from) {
        return new Sensor(from.getSensorId(), from.getVendorId(), from.getSensorType());
    }

    @Override
    public Sensor copy(Sensor from, Sensor reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return Long.BYTES + Long.BYTES + Integer.BYTES;
    }

    @Override
    public void serialize(Sensor record, DataOutputView target) throws IOException {
        target.writeLong(record.getSensorId());
        target.writeLong(record.getVendorId());
        target.writeInt(record.getSensorType().ordinal());
    }

    @Override
    public Sensor deserialize(DataInputView source) throws IOException {
        long sensorId = source.readLong();
        long vendorId = source.readLong();
        Sensor.SensorType sensorType = SENSOR_TYPES[source.readInt()];
        return new Sensor(sensorId, vendorId, sensorType);
    }

    @Override
    public Sensor deserialize(Sensor reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeLong(source.readLong());
        target.writeLong(source.readLong());
        target.writeInt(source.readInt());
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Sensor> snapshotConfiguration() {
        return new SensorSerializerSnapshot();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class SensorSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Sensor> {

        /** Returns a snapshot pointing to the singleton serializer instance. */
        public SensorSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
