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

package com.ververica.flink.training.provided;

import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class EnrichedMeasurement extends SimpleMeasurement {

    private float temperature;

    public EnrichedMeasurement() {}

    public EnrichedMeasurement(
            final int sensorId,
            final double value,
            final String location,
            final float temperature) {
        super(sensorId, value, location);
        this.temperature = temperature;
    }

    public EnrichedMeasurement(final SimpleMeasurement measurement, final float temperature) {
        super(measurement);
        this.temperature = temperature;
    }

    public float getTemperature() {
        return temperature;
    }

    public void setTemperature(float temperature) {
        this.temperature = temperature;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        EnrichedMeasurement that = (EnrichedMeasurement) o;
        return Float.compare(that.temperature, temperature) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), temperature);
    }

    @Override
    public String toString() {
        return "EnrichedMeasurement{" + super.toString() + ", temperature=" + temperature + '}';
    }
}
