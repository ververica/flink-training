package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Duration;

@DoNotChangeThis
public class ShoppingCartFiles {

    public static FileSource<String> makeCartFilesSource(boolean unbounded) throws URISyntaxException {
        URL srcPathAsURL = ShoppingCartFiles.class.getResource("/cart-files/file-000.txt");
        Path srcPath = new Path(srcPathAsURL.toURI());

        // Create a stream of ShoppingCartRecords from the directory we just filled with files.
        FileSource.FileSourceBuilder<String> builder = FileSource.forRecordStreamFormat(
                                new TextLineInputFormat("UTF-8"),
                                srcPath.getParent());

        if (unbounded) {
            builder.monitorContinuously(Duration.ofSeconds(10));
        } else {
            builder.processStaticFileSet();
        }

        return builder.build();
    }

    public static Sink<String> makeResultFilesSink(Path resultsDir) {
        return FileSink.forRowFormat(resultsDir, new SimpleStringEncoder()).build();
    }

    public static FileSource<String> makeResultFilesSource(Path resultsDir) {
        return FileSource.forRecordStreamFormat(new TextLineInputFormat("UTF-8"),
                        resultsDir)
                .build();
    }
}
