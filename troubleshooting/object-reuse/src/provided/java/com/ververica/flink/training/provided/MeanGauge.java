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

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.View;

import com.ververica.flink.training.common.DoNotChangeThis;
import org.apache.commons.math3.stat.descriptive.moment.SecondMoment;

/**
 * Gauge view for determining the mean per time span. Also allows access to min and max metrics via
 * the {@link MinGauge} and {@link MaxGauge} wrappers.
 */
@DoNotChangeThis
public class MeanGauge implements Gauge<Double>, View {

    private final SimpleStats stats = new SimpleStats();
    private SimpleStats currentStats = new SimpleStats();

    @Override
    public void update() {
        currentStats = stats.copy();
        stats.clear();
    }

    /** Adds the given value to the internal statistics. */
    public void addValue(double d) {
        stats.increment(d);
    }

    @Override
    public Double getValue() {
        return currentStats.getMean();
    }

    /**
     * Wraps around the {@link MeanGauge} view to get the <code>min</code> of all reported values.
     */
    public static class MinGauge implements Gauge<Double> {
        private final MeanGauge base;

        /** Creates a min-gauge wrapper around <code>base</code>. */
        public MinGauge(MeanGauge base) {
            this.base = base;
        }

        @Override
        public Double getValue() {
            return base.currentStats.getMin();
        }
    }

    /**
     * Wraps around the {@link MeanGauge} view to get the <code>max</code> of all reported values.
     */
    public static class MaxGauge implements Gauge<Double> {
        private final MeanGauge base;

        /** Creates a max-gauge wrapper around <code>base</code>. */
        public MaxGauge(MeanGauge base) {
            this.base = base;
        }

        @Override
        public Double getValue() {
            return base.currentStats.getMax();
        }
    }

    /** Calculates min, max, mean (first moment), as well as the second moment. */
    private static class SimpleStats extends SecondMoment {
        private static final long serialVersionUID = 1L;

        private double min = Double.NaN;
        private double max = Double.NaN;

        @Override
        public void increment(double d) {
            if (d < min || Double.isNaN(min)) {
                min = d;
            }
            if (d > max || Double.isNaN(max)) {
                max = d;
            }
            super.increment(d);
        }

        @Override
        public SimpleStats copy() {
            SimpleStats result = new SimpleStats();
            SecondMoment.copy(this, result);
            result.min = min;
            result.max = max;
            return result;
        }

        double getMin() {
            return min;
        }

        double getMax() {
            return max;
        }

        double getMean() {
            return m1;
        }
    }
}
