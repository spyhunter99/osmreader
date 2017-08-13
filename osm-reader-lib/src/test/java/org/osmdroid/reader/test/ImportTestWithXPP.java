package org.osmdroid.reader.test;

import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmPullParserReader;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class ImportTestWithXPP extends BaseTests {
    @Override
    public IOsmReader getReader() {
        return new OsmPullParserReader();
    }


    @Override
    public String getOutputFileName() {
        return "importXppBz2.sqlite";
    }

    @Override
    public String getInputFileName() {
        return "delaware-latest.osm.bz2";
    }
}
