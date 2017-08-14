package org.osmdroid.reader;

import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmReaderFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

import static org.osmdroid.reader.Main.formatter;
import static org.osmdroid.reader.Main.toHumanReadableDuration;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */
class ConversionJob implements Runnable {
    File input, output;


    public ConversionJob(File input, File output) {
        this.input = input;
        this.output = output;
    }

    boolean running = true;

    @Override
    public void run() {
        final IOsmReader iOsmReader = OsmReaderFactory.getNewReader();
        try {
            Class.forName("org.sqlite.JDBC").newInstance();

            final long start = System.currentTimeMillis();
            Connection connection = DriverManager.getConnection("jdbc:sqlite:" + output.getAbsolutePath());
            new Thread(new Runnable() {
                @Override
                public void run() {
                    System.out.println(input.getName() + " poller started");
                    System.out.println(input.getName() + " status " + iOsmReader.getProgress() + "% complete");
                    while (running) {
                        try {

                            //it took us this long
                            long elapsedTime = (System.currentTimeMillis() - start);
                            //to get this far into the file
                            double percentDone = iOsmReader.getProgress();

                            //guestimate time remaining
                            long totalEstimatedTimeMs = (long) ((elapsedTime / percentDone) * (100 - percentDone));
                            String readable = toHumanReadableDuration(totalEstimatedTimeMs);
                            System.out.println(input.getName() + " status " + formatter.format(percentDone) + "% complete. Est time remaining: " + readable);
                            Thread.sleep(5000);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
            // delaware-latest.osm.bz2"
            Set<Short> options = new HashSet<Short>();
            options.add(ImportOptions.INCLUDE_RELATIONS);
            options.add(ImportOptions.INCLUDE_WAYS);
            iOsmReader.setOptions(options);
            iOsmReader.setBatchSize(1000);
            iOsmReader.read(input, connection);
            DBUtils.safeClose(connection);
            running = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
