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

package com.ververica.flink.training.solutions;

import java.util.Objects;

@SuppressWarnings({"unused", "RedundantSuppression"})
public class SimpleMeasurement {

    private int sensorId;
    private double value;
    private String location;

    public SimpleMeasurement() {}

    public SimpleMeasurement(final int sensorId, final double value, final String location) {
        this.sensorId = sensorId;
        this.value = value;
        this.location = location;
    }

    public int getSensorId() {
        return sensorId;
    }

    public void setSensorId(final int sensorId) {
        this.sensorId = sensorId;
    }

    public double getValue() {
        return value;
    }

    public void setValue(final double value) {
        this.value = value;
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(final String location) {
        this.location = location;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SimpleMeasurement that = (SimpleMeasurement) o;
        return sensorId == that.sensorId
                && Double.compare(that.value, value) == 0
                && Objects.equals(location, that.location);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, value, location);
    }

    @Override
    public String toString() {
        return "SimpleMeasurement{"
                + "sensorId="
                + sensorId
                + ", value="
                + value
                + ", location='"
                + location
                + '\''
                + '}';
    }
}
