package com.ververica.flink.training.solutions;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;

import java.time.Duration;
import java.util.Collection;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;

/**
 * Extended RocksDB option factory which enables further settings from
 * https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide.
 */
@SuppressWarnings("unused")
public class RocksDBTuningJobOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private static final long serialVersionUID = 1L;

    private static final ConfigOption<Boolean> JAVA_LOGGING =
            key("state.backend.rocksdb.custom.javalogging")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Redirect RocksDB logging to Java (may reduce performance)");

    private static final ConfigOption<Duration> DUMP_STATS_INTERVAL =
            key("state.backend.rocksdb.custom.dumpstats")
                    .durationType()
                    .defaultValue(Duration.ofMinutes(5))
                    .withDescription("If not zero, dump RocksDB stats this often");

    private static final ConfigOption<CompressionType> COMPRESSION =
            key("state.backend.rocksdb.custom.compression")
                    .enumType(CompressionType.class)
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Configures RocksDB compression")
                                    .linebreak()
                                    .text(
                                            "For more information, please refer to %s",
                                            link(
                                                    "https://github.com/facebook/rocksdb/wiki/Compression"))
                                    .build());

    public static final ConfigOption<Integer> MAX_BACKGROUND_JOBS =
            key("state.backend.rocksdb.custom.maxjobs")
                    .intType()
                    .noDefaultValue()
                    .withDescription(
                            "Specifies the maximum number of concurrent background jobs (both flushes and compactions combined). "
                                    + "RocksDB has default configuration as '2'.");

    private final DefaultConfigurableOptionsFactory defaultFactory =
            new DefaultConfigurableOptionsFactory();
    private ReadableConfig configuration;

    @Override
    public DBOptions createDBOptions(
            DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        defaultFactory.setLogLevel(InfoLogLevel.INFO_LEVEL);
        defaultFactory.setLogFileNum(10);
        defaultFactory.setMaxLogFileSize("10MB");
        currentOptions = defaultFactory.createDBOptions(currentOptions, handlesToClose);

        Duration statsDumpPeriod = configuration.get(DUMP_STATS_INTERVAL);
        currentOptions.setStatsDumpPeriodSec(Math.max(1, (int) statsDumpPeriod.toMillis() / 1000));

        configuration
                .getOptional(MAX_BACKGROUND_JOBS)
                .ifPresent(currentOptions::setMaxBackgroundJobs);

        if (configuration.get(JAVA_LOGGING)) {
            Logger logger = new RocksDBNativeLogger(currentOptions);
            handlesToClose.add(logger);
            currentOptions.setLogger(logger);
        }

        return currentOptions;
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        currentOptions = defaultFactory.createColumnOptions(currentOptions, handlesToClose);

        configuration.getOptional(COMPRESSION).ifPresent(currentOptions::setCompressionType);

        return currentOptions;
    }

    @Override
    public RocksDBTuningJobOptionsFactory configure(ReadableConfig configuration) {
        defaultFactory.configure(configuration);
        this.configuration = configuration;
        return this;
    }
}
