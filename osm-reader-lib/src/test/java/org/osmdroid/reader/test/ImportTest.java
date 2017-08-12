package org.osmdroid.reader.test;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.osmdroid.reader.readers.OsmPullParserReader;
import org.osmdroid.reader.QueryTools;
import org.osmdroid.reader.model.SearchResults;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

/** created on 8/12/2017.

 *
 * @author Alex O'Ree
 */

public class ImportTest {

    static Connection connection;
    @BeforeClass
    public static void setupDatabase() throws Exception {
         //static so that it only fires once, since it's slow.
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
            new OsmPullParserReader().read(delaware, connection);
        }
     }

     @Test
    public void testQuery() throws Exception {
         List<SearchResults> searchResults = QueryTools.reverseGeocode("new castle", 10, 0, connection);
         Assert.assertTrue(!searchResults.isEmpty());
         for (int i=0; i < searchResults.size(); i++) {
             SearchResults record = searchResults.get(i);
             System.out.println(record.getName() + " " + searchResults.get(i).getType() + " " + searchResults.get(i).getLat() + "," + searchResults.get(i).getLon());
         }
     }
}
