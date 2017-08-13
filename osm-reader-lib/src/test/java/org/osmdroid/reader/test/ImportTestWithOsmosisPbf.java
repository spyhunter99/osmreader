package org.osmdroid.reader.test;

import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmosisReader;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class ImportTestWithOsmosisPbf extends BaseTests {

    @Override
    public IOsmReader getReader() {
        return new OsmosisReader();
    }

    @Override
    public String getOutputFileName() {
        return "importOsmosisPbf.sqlite";
    }

    @Override
    public String getInputFileName() {
        return "delaware-latest.osm.pbf";
    }

}
