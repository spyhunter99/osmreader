package org.osmdroid.reader;

import com.eaio.util.text.HumanTime;

import org.apache.commons.lang3.text.WordUtils;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmReaderFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 */

public class Main {
static final    NumberFormat formatter = new DecimalFormat("#0.00");

    static boolean running = true;
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.out.println("Usage");
            System.out.println("<input .bz2 file> <output.sqlite file>");
            return;
        }

        final IOsmReader iOsmReader = OsmReaderFactory.getNewReader();
        Class.forName("org.sqlite.JDBC").newInstance();
        final long start = System.currentTimeMillis();
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + args[1]);
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("poller started");
                System.out.println((System.currentTimeMillis() - start) + " status " + iOsmReader.getProgress() + "% complete");
                while (running) {
                    try {
                        long elapsedTime = (System.currentTimeMillis() - start);
                        double percentDone =  iOsmReader.getProgress();
                        long totalEstimatedTimeMs = (long)(((double)elapsedTime/percentDone) * 100d);
                        String readable = toHumanReadableDuration(totalEstimatedTimeMs);
                        System.out.println(elapsedTime + " status " + formatter.format(percentDone) + "% complete. Est time remaining: " + readable);
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        // delaware-latest.osm.bz2"
        Set<Short> opts = new HashSet<Short>();
        //opts.add(ImportOptions.INCLUDE_RELATIONS);
        //  opts.add(ImportOptions.INCLUDE_WAYS);
        iOsmReader.setOptions(opts);
        iOsmReader .read(new File(args[0]), connection);
        running=false;
    }

    private static final List<TimeUnit> timeUnits = Arrays.asList(TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES,
        TimeUnit.SECONDS);

    public static String toHumanReadableDuration(final long millis) {
        final StringBuilder builder = new StringBuilder();
        long acc = millis;
        for (final TimeUnit timeUnit : timeUnits) {
            final long convert = timeUnit.convert(acc, TimeUnit.MILLISECONDS);
            if (convert > 0) {
                builder.append(convert).append(' ').append(WordUtils.capitalizeFully(timeUnit.name())).append(", ");
                acc -= TimeUnit.MILLISECONDS.convert(convert, timeUnit);
            }
        }
        if (builder.length()>1)
            return builder.substring(0, builder.length() - 2);
        return "< 1 sec";
    }
}
