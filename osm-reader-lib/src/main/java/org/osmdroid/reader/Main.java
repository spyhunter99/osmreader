package org.osmdroid.reader;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.text.WordUtils;
import org.osmdroid.reader.downloads.Downloader;
import org.osmdroid.reader.downloads.JsoupHelper;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmReaderFactory;

import java.io.File;
import java.net.URL;
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
    static final NumberFormat formatter = new DecimalFormat("#0.00");

    static boolean running = true;

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("in", true, "The input bz2 or pbf file");
        opts.addOption("out", true, "The output sqlite file");
        opts.addOption("dlp", false, "Prints to stdout all pbf download links for north america");
        opts.addOption("dl", false, "Downloads all pbf files for north america");
        opts.addOption("dli", false, "Downloads and converts all pbf files for north america");

        CommandLineParser parser = new DefaultParser();
        CommandLine parse = parser.parse(opts, args);
        if (parse.hasOption("dlp")) {
            List<String> urls = JsoupHelper.getDownloadlinks("http://download.geofabrik.de/north-america.html");

            for (int i = 0; i < urls.size(); i++) {
                System.out.println(urls.get(i));
            }

        } else if (parse.hasOption("dl")) {
            List<String> urls = JsoupHelper.getDownloadlinks("http://download.geofabrik.de/north-america.html");

            Downloader dl = new Downloader();
            boolean foundCanada=false;
            for (int i = 0; i < urls.size(); i++) {
                System.out.println(urls.get(i));
                if (urls.get(i).contains("canada")){
                    foundCanada=true;
                    continue;
                }
                if (!foundCanada)
                    continue;
                if (urls.get(i).contains("north-america-"))
                    continue;
                System.out.println(urls.get(i));
                URL website = new URL("http://download.geofabrik.de/" + urls.get(i));

                File output = new File(urls.get(i).substring(urls.get(i).lastIndexOf("/")+1));
                if (!output.exists()) {
                    System.out.println("Enqueue download for " + output.getName());
                    dl.enqueue(website.toString(),output);
                }

            }
            System.out.println("Waiting for downloads to finish");
            dl.waitUntilFinished();
            System.out.println("Downloads finished");


        } else if (parse.hasOption("dli")) {
            List<String> urls = JsoupHelper.getDownloadlinks("http://download.geofabrik.de/north-america.html");
            boolean foundCanada=false;
            Downloader dl = new Downloader();
            PooledConvertor convertor = new PooledConvertor();
            for (int i = 0; i < urls.size(); i++) {
                System.out.println(urls.get(i));
                if (urls.get(i).contains("canada")) {
                    foundCanada = true;
                    continue;
                }
                if (!foundCanada)
                    continue;
                if (urls.get(i).contains("north-america-"))
                    continue;
                System.out.println(urls.get(i));
                URL website = new URL("http://download.geofabrik.de/" + urls.get(i));
                File output = new File(urls.get(i).substring(urls.get(i).lastIndexOf("/") + 1));
                if (!output.exists()) {
                    System.out.println("Enqueue download for " + output.getName());
                    dl.enqueue(website.toString(), output);
                }
            }
            System.out.println("Waiting for downloads to finish");
            dl.waitUntilFinished();
            System.out.println("Downloads finished");
            for (int i = 0; i < urls.size(); i++) {
                File output = new File(urls.get(i).substring(urls.get(i).lastIndexOf("/") + 1));
                if (output.exists()) {
                    File db = new File(output.getName() + ".sqlite");
                    if (!db.exists()) {
                        System.out.println("Enqueing conversion for " + output.getName());
                        convertor.enqueue(output, db);
                    }
                }
            }
            System.out.println("Waiting for conversions to finish");
            convertor.waitUntilFinished();
            System.out.println("Conversions finished");

        } else {
            if (!parse.hasOption("in") || !parse.hasOption("out")) {
                HelpFormatter f = new HelpFormatter();
                f.printHelp("java", opts);
                return;
            }
            convert(new File(parse.getOptionValue("in")), new File(parse.getOptionValue("out")));


        }
    }

    private static void convert(File source, File dest) throws Exception{
        final IOsmReader iOsmReader = OsmReaderFactory.getNewReader();
        Class.forName("org.sqlite.JDBC").newInstance();
        final long start = System.currentTimeMillis();
        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + dest.getAbsolutePath());
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("poller started");
                System.out.println((System.currentTimeMillis() - start) + " status " + iOsmReader.getProgress() + "% complete");
                while (running) {
                    try {

                        //it took us this long
                        long elapsedTime = (System.currentTimeMillis() - start);
                        //to get this far into the file
                        double percentDone = iOsmReader.getProgress();

                        //guestimate time remaining
                        long totalEstimatedTimeMs = (long) ((elapsedTime / percentDone) * (100 - percentDone));
                        String readable = toHumanReadableDuration(totalEstimatedTimeMs);
                        System.out.println(elapsedTime + " status " + formatter.format(percentDone) + "% complete. Est time remaining: " + readable);
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
        iOsmReader.read(source, connection);
        DBUtils.safeClose(connection);
        running = false;
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
        if (builder.length() > 1)
            return builder.substring(0, builder.length() - 2);
        return "< 1 sec";
    }


}
