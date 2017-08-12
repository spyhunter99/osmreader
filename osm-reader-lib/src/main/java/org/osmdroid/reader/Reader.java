/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.osmdroid.reader;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;


/**
 * to get a reverse geocode name to lat,lon
 * SELECT * FROM tag where k='name' and v like '%new castle%'
 * <p/>
 * get the refTag.
 * switch refTag
 * case WAY 0
 * road of some sort
 * get the nodes
 * select node_id from way_no where way_id=?
 * select lat,lon from nodes where id=?
 * case NODE 1
 * (towns, cities, POIs)
 * select lat,lon from nodes where id=?
 * case RELATION 2
 *
 * @author alex
 */
public class Reader {

    public static BufferedReader getBufferedReaderForBZ2File(String fileIn) throws FileNotFoundException, CompressorException {
        FileInputStream fin = new FileInputStream(fileIn);

        BufferedInputStream bis = new BufferedInputStream(fin);
        CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
        BufferedReader br2 = new BufferedReader(new InputStreamReader(input));
        return br2;
    }

    enum OsmType {

        WAY, NODE, RELATION;
    }

    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");


    public static class GeoCodeResult {
        public String name;
        public double lat;
        public double lon;
    }

    public List<GeoCodeResult> query(String name, Connection connection) throws Exception {
        List<GeoCodeResult> ret = new ArrayList<GeoCodeResult>();
        PreparedStatement p = connection.prepareStatement("SELECT * FROM tag where k='name' and v like '%?%'");
        p.setString(1, name);
        ResultSet resultSet = p.executeQuery();
        while (resultSet.next()) {

        }


        return ret;
    }

    /**
     * imports the osm bz2 file into the database
     *
     * @param path
     * @param connection
     * @throws Exception if the file wasn't found, can't write the output file, or there's some kind of IO exception while reading
     */
    public void read(String path, Connection connection) throws Exception {

        if (path == null)
            throw new IllegalArgumentException("path");
        if (!new File(path).exists())
            throw new FileNotFoundException("File Not Found");
        PreparedStatement p;
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS \"nodes\" (\"id\" INTEGER PRIMARY KEY  NOT NULL , \"lat\" DOUBLE NOT NULL , \"lon\" DOUBLE NOT NULL , \"version\" INTEGER, \"timestamp\" DATETIME, \"uid\" INTEGER, \"user\" TEXT, \"changeset\" INTEGER)");
            p.execute();
            p.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"relation_members\" (\"type\" TEXT NOT NULL , \"ref\" INTEGER NOT NULL , \"role\" TEXT, \"id\" INTEGER PRIMARY KEY  AUTOINCREMENT  NOT NULL )");
            p.execute();
            p.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"relations\" (\"id\" INTEGER PRIMARY KEY  NOT NULL , \"user\" TEXT, \"uid\" INTEGER, \"version\" INTEGER, \"changeset\" INTEGER, \"timestamp\" DATETIME)");
            p.execute();
            p.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"tag\" (\"id\" INTEGER NOT NULL , \"k\" TEXT NOT NULL , \"v\" TEXT NOT NULL , \"reftype\" INTEGER NOT NULL  DEFAULT -1, PRIMARY KEY( \"reftype\",\"k\" ,\"id\" )   )");
            p.execute();
            p.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"way_no\" (\"way_id\" INTEGER NOT NULL , \"node_id\" INTEGER NOT NULL, PRIMARY KEY (\"way_id\", \"node_id\")  )  ");
            p.execute();
            p.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"ways\" (\"id\" INTEGER PRIMARY KEY  NOT NULL , \"changeset\" INTEGER, \"version\" INTEGER, \"user\" TEXT, \"uid\" INTEGER, \"timestamp\" DATETIME)");
            p.execute();
            p.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        BufferedReader xmlInputStream = getBufferedReaderForBZ2File(path);
        XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
        factory.setNamespaceAware(false);
        XmlPullParser xmlStreamReader = factory.newPullParser();

        xmlStreamReader.setInput(xmlInputStream);


        Stack<String> xpath = new Stack<String>();
        long recordCount = 0;
        long batchCount = 0;
        long lastId = -1;
        long start = System.currentTimeMillis();
        OsmType lastType = OsmType.NODE;
        long id = -1;
        long changset = -1;
        double lat = 0.0;
        double lon = 0.0;
        long version = -1;
        String user = "";
        long uid = -1;
        long inserts=0;
        String key = "";
        String val = "";



        //int eventType = -1;
        Date timestamp = new Date(System.currentTimeMillis());
        //connection.setAutoCommit(false);
        int eventType = xmlStreamReader.getEventType();
        while (eventType != XmlPullParser.END_DOCUMENT) {
            String tagname = xmlStreamReader.getName();
            recordCount++;
            key = "";
            val = "";
            id = -1;
            changset = -1;
            version = -1;
            user = "";
            uid = -1;
            //timestamp = new Date(System.currentTimeMillis());

            //System.out.println(recordCount);
            //System.out.println ("XMLEvent " + eventType + " " + tagname);

            //long btime = System.currentTimeMillis();
            switch (eventType) {
                case XmlPullParser.START_TAG:

                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("osm")) {


                    }

                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("bounds")) {

                    }

                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("node")) {
                        xpath.push(xmlStreamReader.getName().toString());
                        p = connection.prepareStatement("INSERT INTO nodes (id,changeset,version,user,uid,timestamp,lat,lon) "
                                + "VALUES (?,?,?,?,?,?,?,?); ");

                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //		System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //			+ xmlStreamReader.getAttributeValue(i));


                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("id")) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("user")) {
                                user = xmlStreamReader.getAttributeValue(i);
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("uid")) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("changeset")) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("version")) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("lat")) {
                                lat = Double.parseDouble(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("lon")) {
                                lon = Double.parseDouble(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("timestamp")) {
                                timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                            }
                        }
                        if (id != -1) {
                            p.setLong(1, id);
                            p.setLong(2, changset);
                            p.setLong(3, version);
                            p.setString(4, user);
                            p.setLong(5, uid);
                            p.setDate(6, timestamp);
                            p.setDouble(7, lat);
                            p.setDouble(8, lon);
                            try {
                                p.executeUpdate();
                                inserts++;
                                //batchCount++;
                            } catch (Exception ex) {
                                System.out.println(p.toString());
                                ex.printStackTrace();
                            }
                            p.close();
                            p = null;
                        }
                        lastId = id;
                        lastType = OsmType.NODE;
                    } else if (xmlStreamReader.getName().toString().equalsIgnoreCase("relation")) {
                        xpath.push(xmlStreamReader.getName().toString());
                        p = connection.prepareStatement("INSERT INTO relations (id,changeset,version,user,uid,timestamp) "
                                + "VALUES (?,?,?,?,?,?); ");

                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //		System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //			+ xmlStreamReader.getAttributeValue(i));
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("id")) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("user")) {
                                user = xmlStreamReader.getAttributeValue(i);
                            } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("uid")) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("changeset")) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("version")) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("timestamp")) {
                                timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                            } else {
                                System.err.println("relation attrib unhandled " + xmlStreamReader.getAttributeName(i));
                            }
                        }
                        if (id != -1) {
                            p.setLong(1, id);
                            p.setLong(2, changset);
                            p.setLong(3, version);
                            p.setString(4, user);
                            p.setLong(5, uid);
                            p.setDate(6, timestamp);
                            try {
                                p.executeUpdate();
                                //   batchCount++;

                                inserts++;
                            } catch (Exception ex) {
                                System.out.println(p.toString());
                                ex.printStackTrace();
                            }
                            p.close();
                            p = null;
                        }
                        lastId = id;
                        lastType = OsmType.RELATION;
                    } else if (xmlStreamReader.getName().toString().equalsIgnoreCase("way")) {
                        xpath.push(xmlStreamReader.getName().toString());
                        p = connection.prepareStatement("INSERT INTO ways (id,changeset,version,user,uid,timestamp) "
                                + "VALUES (?,?,?,?,?,?); ");

                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //	+ xmlStreamReader.getAttributeValue(i));
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("id")) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("user")) {
                                user = xmlStreamReader.getAttributeValue(i);
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("uid")) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("changeset")) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("version")) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                            }
                            if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("timestamp")) {
                                timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                            }
                        }
                        if (id != -1) {
                            p.setLong(1, id);
                            p.setLong(2, changset);
                            p.setLong(3, version);
                            p.setString(4, user);
                            p.setLong(5, uid);
                            p.setDate(6, timestamp);
                            try {
                                p.executeUpdate();
                                inserts++;
                                //    batchCount++;
                            } catch (Exception ex) {
                                System.out.println(p.toString());
                                ex.printStackTrace();
                            }
                            p.close();
                            p = null;
                        }
                        lastId = id;
                        lastType = OsmType.WAY;

                    } else if (xmlStreamReader.getName().toString().equalsIgnoreCase("tag")) {
                        if (!xpath.isEmpty() && ((xpath.peek().equalsIgnoreCase("way") || xpath.peek().equalsIgnoreCase("node") || xpath.peek().equalsIgnoreCase("relation")) && lastId != -1)) {

                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //	+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("k")) {
                                    key = xmlStreamReader.getAttributeValue(i);
                                } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("v")) {
                                    val = xmlStreamReader.getAttributeValue(i);
                                } else {
                                    //uncaptured attribute
                                    System.out.println(xmlStreamReader.getAttributeName(i) + "=" + xmlStreamReader.getAttributeValue(i));
                                }

                            }
                            if (lastId != -1) {
                                p = connection.prepareStatement("INSERT INTO tag (id,k,v,reftype) "
                                        + "VALUES (?,?,?,?); ");
                                p.setLong(1, lastId);
                                p.setString(2, key);
                                p.setString(3, val);
                                p.setInt(4, lastType.ordinal());
                                try {
                                    p.executeUpdate();
                                    inserts++;
                                } catch (Exception ex) {
                                    System.out.println(p.toString());
                                    ex.printStackTrace();
                                }
                                //  batchCount++;
                                p.close();
                                p = null;
                            } else {
                                System.err.println("ERR0003");
                            }
                        } else {
                            System.err.println("ERR0002");
                        }
                    } else if (xmlStreamReader.getName().toString().equalsIgnoreCase("nd")) {
                        if (xpath.peek().equalsIgnoreCase("way") && lastId != -1) {
                            id = -1;
                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //	System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //		+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("ref")) {
                                    id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                }

                            }
                            if (id != -1) {
                                p = connection.prepareStatement("INSERT INTO way_no (way_id,node_id) "
                                        + "VALUES (?,?); ");
                                p.setLong(1, lastId);
                                p.setLong(2, id);
                                try {
                                    p.executeUpdate();
                                    inserts++;
                                } catch (Exception ex) {
                                    System.out.println(p.toString());
                                    ex.printStackTrace();
                                }
                                p.close();
                                p = null;
                                //batchCount++;
                            }
                        } else {
                            System.err.println("ERR0001");
                        }

                    } else if (xmlStreamReader.getName().toString().equalsIgnoreCase("member")) {

                        if ((xpath.peek().equalsIgnoreCase("relation")) && lastId != -1) {

                            //String type = "";
                            id = -1;
                            //String role = "";
                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //	System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //		+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("type")) {
                                    key = xmlStreamReader.getAttributeValue(i);
                                }
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("ref")) {
                                    id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                }
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("role")) {
                                    val = xmlStreamReader.getAttributeValue(i);
                                }

                            }
                            if (lastId != -1) {
                                p = connection.prepareStatement("INSERT INTO relation_members (id,type,ref,role) "
                                        + "VALUES (?,?,?,?); ");
                                p.setLong(1, lastId);
                                p.setString(2, key);
                                p.setLong(3, id);
                                p.setString(4, val);
                                try {
                                    p.executeUpdate();
                                    inserts++;
                                } catch (Exception ex) {
                                    System.out.println(p.toString());
                                    ex.printStackTrace();
                                }
                                p.close();
                                p = null;
                                // batchCount++;
                            } else {
                                System.err.println("ERR0006");
                            }
                        } else {
                            System.err.println("ERR0005");
                        }

                    } else {
                        System.err.println("unhandled node! " + xmlStreamReader.getName().toString());
                    }

                    break;
                case XmlPullParser.TEXT:
                    //System.out.print("text!" + xmlStreamReader.getText());
                    break;
                case XmlPullParser.END_TAG:
                    //System.out.println("</" + xmlStreamReader.getName().toString() + ">");

                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("node")) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }
                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("way")) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }
                    if (xmlStreamReader.getName().toString().equalsIgnoreCase("relation")) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }

                    break;
                default:
                    //do nothing
                    break;
            }


            //if (batchCount == 100) {
            //connection.commit();
            //  long txPerSecond = (System.currentTimeMillis() - btime);
            System.out.println((start-System.currentTimeMillis()) + " total elements processed " + recordCount + " inserts " + inserts + " stack " + xpath.size());

            //batchCount = 0;
            //System.gc();
            //System.out.println();
            //  }
            eventType = xmlStreamReader.next();
        }

        System.out.println(System.currentTimeMillis()-start + "ms");
    }

    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.out.println("Usage");
            System.out.println("<input .bz2 file> <output.sqlite file>");
            return;
        }
        //2010-03-16T11:47:08Z
        //java.util.Date parse = sdf.parse("2010-03-16T11:47:08Z");
        //System.out.println(parse.toString());


        Connection connection = DriverManager.getConnection("jdbc:sqlite:" + args[1]);

        // delaware-latest.osm.bz2"
        new Reader().read(args[0], connection);
    }


}
