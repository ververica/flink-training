package com.ververica.flink.training.exercises;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory;
import org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;
import org.rocksdb.*;

import java.util.Collection;

/**
 * RocksDB option factory which serves as a template for setting up DB and
 * column options.
 */
public class RocksDBTuningJobOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private final DefaultConfigurableOptionsFactory defaultFactory =
            new DefaultConfigurableOptionsFactory();

    private ReadableConfig configuration;

    @Override
    public RocksDBOptionsFactory configure(ReadableConfig configuration) {
        defaultFactory.configure(configuration);
        this.configuration = configuration;
        return this;
    }

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return defaultFactory.createDBOptions(currentOptions, handlesToClose);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions = defaultFactory.createColumnOptions(currentOptions, handlesToClose);

        // Here is where you could set up LZ4 compression and Bloom filters

        return currentOptions;
    }

}
