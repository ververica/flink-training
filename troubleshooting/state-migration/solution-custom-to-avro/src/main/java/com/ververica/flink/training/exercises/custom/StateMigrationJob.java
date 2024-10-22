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

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.exercises.StateMigrationJobBase;

/** State migration job for custom serializer state migration / schema evolution. */
@DoNotChangeThis
public class StateMigrationJob extends StateMigrationJobBase {

    /**
     * Creates and starts the state migration streaming job.
     *
     * @throws Exception if the application is misconfigured or fails during job submission
     */
    public static void main(String[] args) throws Exception {
        createAndExecuteJob(args, new SensorAggregationProcessing());
    }
}
