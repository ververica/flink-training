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

import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.slf4j.LoggerFactory;

/**
 * RocksDB Logger wrapper to write native log messages to Java instead of RocksDB's own LOG file.
 */
public class RocksDBNativeLogger extends Logger {
    public static final org.slf4j.Logger LOG =
            LoggerFactory.getLogger(RocksDBTuningJobOptionsFactory.class);

    /** Creates a native logger. Make sure to close the native handle after use! */
    public RocksDBNativeLogger(DBOptions currentOptions) {
        super(currentOptions);
    }

    @Override
    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
        switch (infoLogLevel) {
            case DEBUG_LEVEL:
                LOG.debug(logMsg);
                return;
            case INFO_LEVEL:
                LOG.info(logMsg);
                return;
            case WARN_LEVEL:
                LOG.warn(logMsg);
                return;
            case ERROR_LEVEL:
            case FATAL_LEVEL:
                LOG.error(logMsg);
                return;
            case HEADER_LEVEL:
            case NUM_INFO_LOG_LEVELS:
        }
    }
}
