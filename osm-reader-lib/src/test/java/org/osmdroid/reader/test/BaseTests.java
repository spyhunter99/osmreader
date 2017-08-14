package org.osmdroid.reader.test;

import org.junit.Assert;
import org.junit.Test;
import org.osmdroid.reader.model.Address;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.QueryTools;
import org.osmdroid.reader.model.SearchResults;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.osmdroid.reader.Main.toHumanReadableDuration;

/** created on 8/12/2017.

 *
 * @author Alex O'Ree
 */

public abstract class BaseTests {
     final NumberFormat formatter = new DecimalFormat("#0.00");
     boolean running=true;
     long start = System.currentTimeMillis();
     Connection connection;

    public abstract IOsmReader getReader();
    public abstract String getOutputFileName();
    public abstract String getInputFileName();

     public void init() throws Exception{
         //static so that it only fires once, since it's slow.

         //this bundle of joy is here due to the changing of relatives paths of the gradle/android studio does
         //the working directory is not that of the module but that of whereever you run the gradle command from

         //maven does not do this non-sense and always normalized on the directory of the module
         File delaware = new File("src/test/resources/" + getInputFileName());
         if (!delaware.exists())
             delaware = new File("osm-reader-lib/src/test/resources/" + getInputFileName());
         if (!delaware.exists())
             throw new Exception(delaware.getAbsolutePath() + " does not exist");

         File output = new File("build/" + getOutputFileName());
         if (output.exists()) {
             System.out.println("skipping import since the file exists already");
             connection = DriverManager.getConnection("jdbc:sqlite:" + output.getAbsolutePath());
         } else {
             connection = DriverManager.getConnection("jdbc:sqlite:" + output.getAbsolutePath());
             final IOsmReader osmPullParserReader = getReader();
             osmPullParserReader.setBatchSize(500);
             Set<Short> opts = new HashSet<Short>();
             opts.add(ImportOptions.INCLUDE_WAYS);
             opts.add(ImportOptions.INCLUDE_RELATIONS);
             osmPullParserReader.setOptions(opts);
             new Thread(new Runnable() {
                 @Override
                 public void run() {
                     System.out.println("poller started");
                     System.out.println((System.currentTimeMillis() - start) + " status " + osmPullParserReader.getProgress() + "% complete");
                     while (running) {
                         try {
                             long elapsedTime = (System.currentTimeMillis() - start);
                             double percentDone =  osmPullParserReader.getProgress();
                             long totalEstimatedTimeMs = (long)((elapsedTime/percentDone) * (100-percentDone));
                             String readable = toHumanReadableDuration(totalEstimatedTimeMs);
                             System.out.println(osmPullParserReader.getParserName() + " " + elapsedTime + " status " + formatter.format(percentDone) + "% complete. Est time remaining: " + readable);
                             Thread.sleep(5000);
                         } catch (Exception e) {
                             e.printStackTrace();
                         }
                     }
                 }
             }).start();
             osmPullParserReader .read(delaware, connection);
             System.out.println("Import complete in  " + (System.currentTimeMillis() - start) + "ms");
             System.out.println("Total inserts " + osmPullParserReader.getInserts());
             System.out.println("XML records processed " + osmPullParserReader.getRecordsProcessed());
             running=false;
         }
     }

    @Test
    public void testQuery() throws Exception {
        init();
         List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection);
         Assert.assertTrue(!searchResults.isEmpty());
         for (int i=0; i < searchResults.size(); i++) {
             SearchResults record = searchResults.get(i);
             System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
         }
     }


    @Test
    public void testQueryBounds() throws Exception {
        init();
        List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection, 90,180,-90,-180);
        Assert.assertTrue(!searchResults.isEmpty());
        for (int i=0; i < searchResults.size(); i++) {
            SearchResults record = searchResults.get(i);
            System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
        }
    }


    @Test
    public void testQueryGetTags() throws Exception {
        init();
        List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection, 40,-75,39,-76);
        Assert.assertTrue(!searchResults.isEmpty());
        for (int i=0; i < searchResults.size(); i++) {
            SearchResults record = searchResults.get(i);
            System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon());
            Map<String, String> tags = QueryTools.getTags(record.getDatabaseId(), connection, 200, 0);
            Assert.assertTrue(!tags.isEmpty());
            Iterator<Map.Entry<String, String>> iterator = tags.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> next = iterator.next();
                System.out.println("\t" + next.getKey() + "=" + next.getValue());
            }
        }
    }

    @Test
    public void getDetails() throws Exception{
        init();
        Map<String, String> tags = QueryTools.getTags(366753278L, connection, 1000, 0);
        Assert.assertEquals(tags.size(), 15);
        Iterator<Map.Entry<String, String>> iterator = tags.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next = iterator.next();
            System.out.println(next.getKey() + " = " + next.getValue());
        }
    }

    @Test
    public void getAddress() throws Exception{
        init();
        Address addr = QueryTools.getAddress(366753278L, connection);
        Assert.assertNotNull(addr);
        Assert.assertEquals("19730", addr.getAddr_postcode());
        Assert.assertEquals("+1-302-378-5749", addr.getPhone());
        Assert.assertEquals("http://dsp.delaware.gov/index.shtml", addr.getWebsite());

        Assert.assertEquals("Odessa", addr.getAddr_city());
        Assert.assertEquals("DE", addr.getAddr_state());
        Assert.assertEquals("Main Street", addr.getAddr_street());
        Assert.assertEquals("414", addr.getAddr_housenumber());
        Assert.assertEquals("New Castle", addr.getAddr_county());

    }
}
