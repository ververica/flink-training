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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import com.ververica.flink.training.common.DoNotChangeThis;

import java.io.IOException;

/** Custom serializer base class providing some default implementations for convenience. */
@DoNotChangeThis
public abstract class AggregatedSensorStatisticsSerializerBase<T> extends TypeSerializer<T> {
    private static final long serialVersionUID = -6967834481356870922L;

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public T copy(T from, T reuse) {
        return copy(from);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        serialize(deserialize(source), target);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        return this;
    }

    @Override
    public int hashCode() {
        return this.getClass().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof AggregatedSensorStatisticsSerializerBase) {
            AggregatedSensorStatisticsSerializerBase<?> other =
                    (AggregatedSensorStatisticsSerializerBase<?>) obj;

            return other.getClass().equals(getClass());
        } else {
            return false;
        }
    }
}
