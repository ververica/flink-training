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

public class LocationSerializer extends TypeSerializerSingleton<Location> {

    private LocationSerializer() {}

    static final LocationSerializer INSTANCE = new LocationSerializer();

    @Override
    public boolean isImmutableType() {
        return true;
    }

    @Override
    public Location createInstance() {
        return null;
    }

    @Override
    public Location copy(Location from) {
        return new Location(from.getLongitude(), from.getLatitude(), from.getHeight());
    }

    @Override
    public Location copy(Location from, Location reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return Double.BYTES + Double.BYTES + Double.BYTES;
    }

    @Override
    public void serialize(Location record, DataOutputView target) throws IOException {
        target.writeDouble(record.getLongitude());
        target.writeDouble(record.getLatitude());
        target.writeDouble(record.getHeight());
    }

    @Override
    public Location deserialize(DataInputView source) throws IOException {
        double longitude = source.readDouble();
        double latitude = source.readDouble();
        double height = source.readDouble();
        return new Location(longitude, latitude, height);
    }

    @Override
    public Location deserialize(Location reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeDouble(source.readDouble());
        target.writeDouble(source.readDouble());
        target.writeDouble(source.readDouble());
    }

    // -----------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Location> snapshotConfiguration() {
        return new LocationSerializerSnapshot();
    }

    @SuppressWarnings("WeakerAccess")
    public static final class LocationSerializerSnapshot
            extends SimpleTypeSerializerSnapshot<Location> {

        /** Returns a snapshot pointing to the singleton serializer instance. */
        public LocationSerializerSnapshot() {
            super(() -> INSTANCE);
        }
    }
}
