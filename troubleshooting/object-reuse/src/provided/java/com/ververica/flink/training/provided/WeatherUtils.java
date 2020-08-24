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

import com.ververica.flink.training.common.DoNotChangeThis;

/** Various tools to convert units used in weather sensors. */
@DoNotChangeThis
public class WeatherUtils {

    /** Converts the given temperature from Fahrenheit to Celcius. */
    public static double fahrenheitToCelcius(double temperatureInFahrenheit) {
        return ((temperatureInFahrenheit - 32) * 5.0) / 9.0;
    }

    /** Converts the given temperature from Celcius to Fahrenheit. */
    public static double celciusToFahrenheit(double celcius) {
        return (celcius * 9.0) / 5.0 + 32;
    }

    /** Miles per hour -> kilometres per hour. */
    public static double mphToKph(double mph) {
        return mph * 1.60934;
    }

    /** Kilometres per hour -> miles per hour. */
    public static double kphToMph(double kph) {
        return kph / 1.60934;
    }
}
