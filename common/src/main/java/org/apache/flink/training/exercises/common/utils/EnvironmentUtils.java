package org.apache.flink.training.exercises.common.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class EnvironmentUtils {
    public static StreamExecutionEnvironment getStreamExecutionEnvironment(boolean useLocalEnvironment) {

        if (useLocalEnvironment) {
            // Create a local environment with a Web UI
            Configuration conf = new Configuration();
            conf.setString("metrics.reporter.influxdb.factory.class","org.apache.flink.metrics.influxdb.InfluxdbReporterFactory");
            conf.setString("metrics.reporter.influxdb.scheme","http");
            conf.setString("metrics.reporter.influxdb.host","localhost");
            conf.setString("metrics.reporter.influxdb.port","8086");
            conf.setString("metrics.reporter.influxdb.db","flink");
            conf.setString("metrics.reporter.influxdb.username","flink-metrics");
            conf.setString("metrics.reporter.influxdb.password","qwerty");
            conf.setString("metrics.reporter.influxdb.retentionPolicy","one_hour");
            conf.setString("metrics.reporter.influxdb.consistency","ANY");
            conf.setString("metrics.reporter.influxdb.connectTimeout","60000");
            conf.setString("metrics.reporter.influxdb.writeTimeout","60000");
            conf.setString("metrics.reporter.influxdb.interval","60 SECONDS");

            return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        } else {
            // set up streaming execution environment without Web UI
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
    
}
