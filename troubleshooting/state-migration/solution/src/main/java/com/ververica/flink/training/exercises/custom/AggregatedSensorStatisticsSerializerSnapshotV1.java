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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

/**
 * Serializer configuration snapshot for POJO and format evolution.
 */
public final class AggregatedSensorStatisticsSerializerSnapshotV1 implements
		TypeSerializerSnapshot<AggregatedSensorStatistics> {
	@Override
	public int getCurrentVersion() {
		return 1;
	}

	@Override
	public TypeSerializerSchemaCompatibility<AggregatedSensorStatistics> resolveSchemaCompatibility(
			TypeSerializer<AggregatedSensorStatistics> newSerializer) {
		if (newSerializer instanceof AggregatedSensorStatisticsSerializerV1) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		} else if (newSerializer instanceof AggregatedSensorStatisticsSerializerV2) {
			return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
		} else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public TypeSerializer<AggregatedSensorStatistics> restoreSerializer() {
		return new AggregatedSensorStatisticsSerializerV1();
	}

	@Override
	public void writeSnapshot(DataOutputView out) {
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {
	}
}
