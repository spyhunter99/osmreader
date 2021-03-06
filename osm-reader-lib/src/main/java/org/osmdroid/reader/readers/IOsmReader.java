package org.osmdroid.reader.readers;

import org.osmdroid.reader.model.ImportOptions;

import java.io.File;
import java.sql.Connection;
import java.util.Set;

/**
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 */

public interface IOsmReader {
    /**
     * gets at estimate for completion.
     * @return 0-100 or -1 if reading has not yet been started
     */

    double getProgress();

    /**
     * this is expected to block until the input file has been read completely and all
     * records imported into the database
     * @param path
     * @param connection
     * @throws Exception
     */
    void read(File path, Connection connection) throws Exception;


    String getParserName();

    /**
     * Import options, skip the stuff you don't need
     * @see ImportOptions
     * @param options
     */
    void setOptions(Set<Short> options);

    long getInserts();
    long getRecordsProcessed();
    void setBatchSize(int size);
}
