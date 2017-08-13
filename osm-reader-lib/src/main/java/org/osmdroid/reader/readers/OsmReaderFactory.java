package org.osmdroid.reader.readers;

/**
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 */

public class OsmReaderFactory {
    public static IOsmReader getNewReader(){
        //if needed, we can have multiple options here. apparently android is faster with a sax parser
        //return new OsmPullParserReader();
        return new OsmosisReader();
        /*
        //TODO
        //if (android), return sax parser
        //else return pull parser
        try {
            Class.forName("javax.swing.AbstractAction").newInstance();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return new OsmSaxParser();*/
    }
}
