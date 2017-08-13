package org.osmdroid.reader.test;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmPullParserReader;
import org.osmdroid.reader.QueryTools;
import org.osmdroid.reader.model.SearchResults;
import org.osmdroid.reader.readers.OsmReaderFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** created on 8/12/2017.

 *
 * @author Alex O'Ree
 */

public class ImportTest {

    static Connection connection;
    @BeforeClass
    public static void setupDatabase() throws Exception {
         //static so that it only fires once, since it's slow.

        //this bundle of joy is here due to the changing of relatives paths of the gradle/android studio does
        //the working directory is not that of the module but that of whereever you run the gradle command from

        //maven does not do this non-sense and always normalized on the directory of the module
         File delaware = new File("src/test/resources/delaware-latest.osm.bz2");
         if (!delaware.exists())
             delaware = new File("osm-reader-lib/src/test/resources/delaware-latest.osm.bz2");
        if (!delaware.exists())
             throw new Exception(delaware.getAbsolutePath() + " does not exist");

        File output = new File("build/import.sqlite");
        if (output.exists()) {
            System.out.println("skipping import since the file exists already");
            connection = DriverManager.getConnection("jdbc:sqlite:" + output.getAbsolutePath());
        } else {
            connection = DriverManager.getConnection("jdbc:sqlite:" + output.getAbsolutePath());
            IOsmReader osmPullParserReader = OsmReaderFactory.getNewReader();
            Set<Short> opts = new HashSet<Short>();
            opts.add(ImportOptions.INCLUDE_WAYS);
            opts.add(ImportOptions.INCLUDE_RELATIONS);
            osmPullParserReader.setOptions(opts);
            osmPullParserReader .read(delaware, connection);
        }
     }

    @Test
    public void testQuery() throws Exception {
         List<SearchResults> searchResults = QueryTools.reverseGeocode("new castle", 10, 0, connection);
         Assert.assertTrue(!searchResults.isEmpty());
         for (int i=0; i < searchResults.size(); i++) {
             SearchResults record = searchResults.get(i);
             System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
         }
     }


    @Test
    public void testQueryBounds() throws Exception {
        List<SearchResults> searchResults = QueryTools.search("new castle", 10, 0, connection, 90,180,-90,-180);
        Assert.assertTrue(!searchResults.isEmpty());
        for (int i=0; i < searchResults.size(); i++) {
            SearchResults record = searchResults.get(i);
            System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon() + "," + searchResults.get(i).getDatabaseId());
        }
    }


    @Test
    public void testQueryGetTags() throws Exception {
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
}
