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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

class MeasurementValueTypeInfo extends TypeInformation<MeasurementValue> {

    private MeasurementValueTypeInfo() {}

    static final MeasurementValueTypeInfo INSTANCE = new MeasurementValueTypeInfo();

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 3;
    }

    @Override
    public int getTotalFields() {
        return 3;
    }

    @Override
    public Class<MeasurementValue> getTypeClass() {
        return MeasurementValue.class;
    }

    @Override
    public boolean isKeyType() {
        return true;
    }

    @Override
    public TypeSerializer<MeasurementValue> createSerializer(ExecutionConfig config) {
        return MeasurementValueSerializer.INSTANCE;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(Object obj) {
        return this.canEqual(obj);
    }

    @Override
    public int hashCode() {
        return MeasurementValue.class.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof MeasurementValueTypeInfo;
    }
}
