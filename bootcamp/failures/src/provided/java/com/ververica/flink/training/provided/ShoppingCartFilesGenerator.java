package com.ververica.flink.training.provided;

import com.ververica.flink.training.common.DoNotChangeThis;
import com.ververica.flink.training.common.ShoppingCartGenerator;
import com.ververica.flink.training.common.ShoppingCartRecord;
import org.apache.flink.util.Preconditions;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.zip.GZIPOutputStream;

/**
 * A utility class that we use to create a large set of files containing
 * shopping cart records, for our failures lab.
 */
@DoNotChangeThis
public class ShoppingCartFilesGenerator {

    private static final long START_TIME = 0;

    public static void main(String[] args) throws Exception {
        Path srcDir = Files.createTempDirectory("shopping-cart-files");
        final long numRecords = 100_000;
        final long numFiles = 1;
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);
        ShoppingCartFilesGenerator.generateFiles(generator, srcDir.toFile(), numRecords, numFiles, 0.1);

        System.out.println("Generated files to " + srcDir);
    }

    public static void main2(String[] args) throws Exception {
        Path srcDir = Files.createTempDirectory("shopping-cart-files");
        final long numRecords = 1_000;
        final long numFiles = 10;
        ShoppingCartGenerator generator = new ShoppingCartGenerator(START_TIME);
        ShoppingCartFilesGenerator.generateFiles(generator, srcDir.toFile(), numRecords, numFiles, 0.0);

    }

    public static void generateFiles(ShoppingCartGenerator generator, File dir, long numRecords, long numFiles, double percentageAbandoned) throws Exception {
        Preconditions.checkArgument(numRecords > 0, "Num records must be > 0");
        Preconditions.checkArgument(numRecords >= numFiles, "Num records must be >= num files");

        dir.mkdirs();

        Random rand = new Random(410);

        int fileIndex = 0;
        long recordsWritten = 0;
        while (recordsWritten < numRecords) {
            fileIndex++;
            long recordsInFile = (numRecords - recordsWritten) / numFiles;

            File out = new File(dir, String.format("file-%03d.gz", fileIndex));
            GZIPOutputStream bos = new GZIPOutputStream(new FileOutputStream(out));
            WritableByteChannel channel = Channels.newChannel(bos);

            for (long i = 0; i < recordsInFile; i++) {
                ShoppingCartRecord cart = generator.apply(recordsWritten++);
                if ((percentageAbandoned > 0.0) && (rand.nextDouble() < percentageAbandoned)) {
                    cart.setTransactionCompleted(false);
                }

                channel.write(StandardCharsets.UTF_8.encode(cart.toString()));
                channel.write(StandardCharsets.UTF_8.encode("\n"));
            }

            bos.close();
            numFiles--;
        }

        // Generate two final, not completed transactions in the future that will force
        // windows to complete.
        File out = new File(dir, String.format("file-%03d.txt", fileIndex + 1));
        FileOutputStream fos = new FileOutputStream(out);
        ShoppingCartRecord finalCart = generator.createShoppingCart();
        finalCart.setTransactionTime(numRecords * 10 * 100L);
        fos.write(finalCart.toString().getBytes(StandardCharsets.UTF_8));
        fos.write("\n".getBytes(StandardCharsets.UTF_8));
        fos.write(finalCart.toString().getBytes(StandardCharsets.UTF_8));
        fos.write("\n".getBytes(StandardCharsets.UTF_8));
        fos.close();
    }
}
