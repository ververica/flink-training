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

package com.ververica.flink.training.exercises.custom;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

public class AggregatedSensorStatisticsSerializerV1
        extends AggregatedSensorStatisticsSerializerBase<AggregatedSensorStatistics> {

    private static final long serialVersionUID = 7089080352317847665L;

    @Override
    public AggregatedSensorStatistics createInstance() {
        return new AggregatedSensorStatistics();
    }

    @Override
    public void serialize(AggregatedSensorStatistics record, DataOutputView target)
            throws IOException {
        target.writeInt(record.getSensorId());
        target.writeLong(record.getCount());
        target.writeLong(record.getLastUpdate());
    }

    @Override
    public AggregatedSensorStatistics deserialize(DataInputView source) throws IOException {
        int sensorId = source.readInt();
        long count = source.readLong();
        long lastUpdate = source.readLong();

        AggregatedSensorStatistics result = new AggregatedSensorStatistics();
        result.setSensorId(sensorId);
        result.setCount(count);
        result.setLastUpdate(lastUpdate);
        return result;
    }

    @Override
    public AggregatedSensorStatistics copy(AggregatedSensorStatistics from) {
        AggregatedSensorStatistics result = new AggregatedSensorStatistics();
        result.setSensorId(from.getSensorId());
        result.setCount(from.getCount());
        result.setLastUpdate(from.getLastUpdate());
        return result;
    }

    @Override
    public TypeSerializerSnapshot<AggregatedSensorStatistics> snapshotConfiguration() {
        return new AggregatedSensorStatisticsSerializerSnapshotV1();
    }
}
