package org.osmdroid.reader.test;

import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmosisReader;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class ImportTestWithOsmosis extends BaseTests {

    @Override
    public IOsmReader getReader() {
        return new OsmosisReader();
    }

    @Override
    public String getOutputFileName() {
        return "importOsmosisBz2.sqlite";
    }

    @Override
    public String getInputFileName() {
        return "delaware-latest.osm.bz2";
    }

}
