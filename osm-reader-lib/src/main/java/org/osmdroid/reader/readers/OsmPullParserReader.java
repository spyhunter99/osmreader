/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.osmdroid.reader.readers;

import org.osmdroid.reader.DBUtils;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.model.OsmType;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Set;
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
public class OsmPullParserReader implements IOsmReader {

    public static final String USER = "user";
    public static final String UID = "uid";
    public static final String CHANGESET = "changeset";
    public static final String VERSION = "version";
    public static final String TIMESTAMP = "timestamp";
    public static final String NODE = "node";
    public static final String RELATION = "relation";
    public static final String TAG = "tag";
    public static final String WAY = "way";
    public static final String MEMBER = "member";
    public static final String ID = "id";
    public static final String LAT = "lat";
    public static final String LON = "lon";
    public static final String BOUNDS = "bounds";
    public static final String OSM = "osm";


    static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");


    private boolean isReading = false;
    private long recordCount = 0;
    private long inputFileSize = 0;
    private Set<Short> options = new HashSet<Short>();

    /**
     * gets at estimate for completion.
     *
     * @return
     */
    public double getProgress() {
        if (isReading) {
            //delaware has 11000000 bytes
            //with 6119693 xml elements
            long expectedRecordCount = (long)(double) ((double) inputFileSize * 6119693d / 11000000d );

            double value = (((double)recordCount)/ expectedRecordCount) * 100d;

            return value;

        }
        return -1;
    }

    /**
     * imports the osm bz2 file into the database
     *
     * @param path
     * @param connection
     * @throws Exception if the file wasn't found, can't write the output file, or there's some kind of IO exception while reading
     */
    public synchronized void read(File path, Connection connection) throws Exception {

        if (path == null)
            throw new IllegalArgumentException("path");
        if (!path.exists())
            throw new FileNotFoundException("File Not Found");
        inputFileSize = path.length();
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

        BufferedReader xmlInputStream = DBUtils.getBufferedReaderForBZ2File(path);
        XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
        factory.setNamespaceAware(false);
        XmlPullParser xmlStreamReader = factory.newPullParser();

        xmlStreamReader.setInput(xmlInputStream);
        isReading = true;
        Stack<String> xpath = new Stack<String>();
        recordCount = 0;
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
        long inserts = 0;
        String key = "";
        String val = "";

        PreparedStatement INSERT_NODES = connection.prepareStatement("INSERT INTO nodes (id,changeset,version,user,uid,timestamp,lat,lon) "
            + "VALUES (?,?,?,?,?,?,?,?); ");

        PreparedStatement INSERT_RELATIONS = connection.prepareStatement("INSERT INTO relations (id,changeset,version,user,uid,timestamp) "
            + "VALUES (?,?,?,?,?,?); ");

        PreparedStatement INSERT_WAYS = connection.prepareStatement("INSERT INTO way_no (way_id,node_id) "
            + "VALUES (?,?); ");

        PreparedStatement INSERT_TAG = connection.prepareStatement("INSERT INTO tag (id,k,v,reftype) "
            + "VALUES (?,?,?,?); ");

        PreparedStatement INSERT_WAY_NO = connection.prepareStatement("INSERT INTO way_no (way_id,node_id) "
            + "VALUES (?,?); ");

        PreparedStatement INSERT_RELATION_MEMBER = connection.prepareStatement("INSERT INTO relation_members (id,type,ref,role) "
            + "VALUES (?,?,?,?); ");

        Date timestamp = new Date(System.currentTimeMillis());

        int eventType = xmlStreamReader.getEventType();
        while (eventType != XmlPullParser.END_DOCUMENT) {
            //String tagname = xmlStreamReader.getName();
            recordCount++;
            key = "";
            val = "";
            id = -1;
            changset = -1;
            version = -1;
            user = "";
            uid = -1;

            switch (eventType) {
                case XmlPullParser.START_TAG:

                    if (OSM.equalsIgnoreCase(xmlStreamReader.getName().toString())) {

                    } else if (BOUNDS.equalsIgnoreCase(xmlStreamReader.getName().toString())) {

                    } else if (NODE.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        xpath.push(xmlStreamReader.getName().toString());

                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //		System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //			+ xmlStreamReader.getAttributeValue(i));


                            if (ID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (USER.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                user = xmlStreamReader.getAttributeValue(i);
                                continue;
                            } else if (UID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (CHANGESET.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (VERSION.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (LAT.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                lat = Double.parseDouble(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (LON.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                lon = Double.parseDouble(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (TIMESTAMP.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                try {
                                    timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    timestamp.setTime(0);
                                }
                                continue;
                            }
                        }
                        if (id != -1) {
                            INSERT_NODES.setLong(1, id);
                            INSERT_NODES.setLong(2, changset);
                            INSERT_NODES.setLong(3, version);
                            INSERT_NODES.setString(4, user);
                            INSERT_NODES.setLong(5, uid);
                            INSERT_NODES.setDate(6, timestamp);
                            INSERT_NODES.setDouble(7, lat);
                            INSERT_NODES.setDouble(8, lon);
                            try {
                                INSERT_NODES.executeUpdate();
                                INSERT_NODES.clearParameters();
                                inserts++;
                                //batchCount++;
                            } catch (Exception ex) {
                                System.out.println(INSERT_NODES.toString());
                                ex.printStackTrace();
                                DBUtils.safeClose(INSERT_NODES);
                                ;
                                INSERT_NODES = connection.prepareStatement("INSERT INTO nodes (id,changeset,version,user,uid,timestamp,lat,lon) "
                                    + "VALUES (?,?,?,?,?,?,?,?); ");
                            }


                        }
                        lastId = id;
                        lastType = OsmType.NODE;
                    } else if (RELATION.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        xpath.push(xmlStreamReader.getName().toString());


                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //		System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //			+ xmlStreamReader.getAttributeValue(i));
                            if (ID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (USER.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                user = xmlStreamReader.getAttributeValue(i);
                                continue;
                            } else if (UID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (CHANGESET.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (VERSION.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (TIMESTAMP.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                try {
                                    timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    timestamp.setTime(0);
                                }
                                continue;
                            } else {
                                System.err.println("relation attrib unhandled " + xmlStreamReader.getAttributeName(i));
                            }
                        }
                        if (id != -1) {
                            if (options.contains(ImportOptions.INCLUDE_RELATIONS)) {

                                INSERT_RELATIONS.setLong(1, id);
                                INSERT_RELATIONS.setLong(2, changset);
                                INSERT_RELATIONS.setLong(3, version);
                                INSERT_RELATIONS.setString(4, user);
                                INSERT_RELATIONS.setLong(5, uid);
                                INSERT_RELATIONS.setDate(6, timestamp);
                                try {
                                    INSERT_RELATIONS.executeUpdate();
                                    INSERT_RELATIONS.clearParameters();
                                    //   batchCount++;

                                    inserts++;
                                } catch (Exception ex) {
                                    System.out.println(INSERT_RELATIONS.toString());
                                    ex.printStackTrace();
                                    INSERT_RELATIONS.clearWarnings();
                                }


                            }
                        }
                        lastId = id;
                        lastType = OsmType.RELATION;
                    } else if (WAY.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        xpath.push(xmlStreamReader.getName().toString());


                        for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                            //System.out.println(xmlStreamReader.getAttributeName(i) + "="
                            //	+ xmlStreamReader.getAttributeValue(i));
                            if (ID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (USER.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                user = xmlStreamReader.getAttributeValue(i);
                                continue;
                            } else if (UID.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (CHANGESET.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (VERSION.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                continue;
                            } else if (TIMESTAMP.equalsIgnoreCase(xmlStreamReader.getAttributeName(i))) {
                                try {
                                    timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                                } catch (Exception ex) {
                                    ex.printStackTrace();
                                    timestamp.setTime(0);
                                }
                                continue;
                            }
                        }
                        if (id != -1) {
                            if (options.contains(ImportOptions.INCLUDE_WAYS)) {

                                INSERT_WAYS.setLong(1, id);
                                INSERT_WAYS.setLong(2, changset);
                                INSERT_WAYS.setLong(3, version);
                                INSERT_WAYS.setString(4, user);
                                INSERT_WAYS.setLong(5, uid);
                                INSERT_WAYS.setDate(6, timestamp);
                                try {
                                    INSERT_WAYS.executeUpdate();
                                    INSERT_WAYS.clearParameters();
                                    inserts++;
                                    //    batchCount++;
                                } catch (Exception ex) {
                                    System.out.println(INSERT_WAYS.toString());
                                    ex.printStackTrace();
                                    INSERT_WAYS.clearWarnings();
                                }

                            }
                        }
                        lastId = id;
                        lastType = OsmType.WAY;

                    } else if (TAG.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        if (!xpath.isEmpty() && ((WAY.equalsIgnoreCase(xpath.peek()) ||
                            NODE.equalsIgnoreCase(xpath.peek()) ||
                            RELATION.equalsIgnoreCase(xpath.peek())) && lastId != -1)) {

                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //	+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("k")) {
                                    key = xmlStreamReader.getAttributeValue(i);
                                    continue;
                                } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("v")) {
                                    val = xmlStreamReader.getAttributeValue(i);
                                    continue;
                                } else {
                                    //uncaptured attribute
                                    System.out.println(xmlStreamReader.getAttributeName(i) + "=" + xmlStreamReader.getAttributeValue(i));
                                }

                            }
                            if (lastId != -1) {

                                INSERT_TAG.setLong(1, lastId);
                                INSERT_TAG.setString(2, key);
                                INSERT_TAG.setString(3, val);
                                INSERT_TAG.setInt(4, lastType.ordinal());
                                try {
                                    INSERT_TAG.executeUpdate();
                                    INSERT_TAG.clearParameters();
                                    inserts++;
                                } catch (Exception ex) {
                                    System.out.println(INSERT_TAG.toString());
                                    ex.printStackTrace();
                                    INSERT_TAG.clearWarnings();
                                }

                            } else {
                                System.err.println("ERR0003");
                            }
                        } else {
                            System.err.println("ERR0002");
                        }
                    } else if ("nd".equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        if (WAY.equalsIgnoreCase(xpath.peek()) && lastId != -1) {
                            id = -1;
                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //	System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //		+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("ref")) {
                                    id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                }

                            }
                            if (id != -1) {
                                if (options.contains(ImportOptions.INCLUDE_WAYS)) {
                                    INSERT_WAY_NO.setLong(1, lastId);
                                    INSERT_WAY_NO.setLong(2, id);
                                    try {
                                        INSERT_WAY_NO.executeUpdate();
                                        INSERT_WAY_NO.clearParameters();
                                        inserts++;
                                    } catch (Exception ex) {
                                        System.out.println(INSERT_WAY_NO.toString());
                                        ex.printStackTrace();
                                        INSERT_WAY_NO.clearWarnings();
                                    }

                                }
                            }
                        } else {
                            System.err.println("ERR0001");
                        }

                    } else if (MEMBER.equalsIgnoreCase(xmlStreamReader.getName().toString())) {

                        if ((RELATION.equalsIgnoreCase(xpath.peek())) && lastId != -1) {

                            //String type = "";
                            id = -1;
                            //String role = "";
                            for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                //	System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                //		+ xmlStreamReader.getAttributeValue(i));
                                if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("type")) {
                                    key = xmlStreamReader.getAttributeValue(i);
                                    continue;
                                } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("ref")) {
                                    id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                    continue;
                                } else if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("role")) {
                                    val = xmlStreamReader.getAttributeValue(i);
                                    continue;
                                }

                            }
                            if (lastId != -1) {

                                if (options.contains(ImportOptions.INCLUDE_RELATIONS)) {

                                    INSERT_RELATION_MEMBER.setLong(1, lastId);
                                    INSERT_RELATION_MEMBER.setString(2, key);
                                    INSERT_RELATION_MEMBER.setLong(3, id);
                                    INSERT_RELATION_MEMBER.setString(4, val);
                                    try {
                                        INSERT_RELATION_MEMBER.executeUpdate();
                                        INSERT_RELATION_MEMBER.clearParameters();
                                        inserts++;
                                    } catch (Exception ex) {
                                        System.out.println(INSERT_RELATION_MEMBER.toString());
                                        ex.printStackTrace();
                                        INSERT_RELATION_MEMBER.clearWarnings();
                                    }

                                }
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

                    if (NODE.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }
                    if (WAY.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }
                    if (RELATION.equalsIgnoreCase(xmlStreamReader.getName().toString())) {
                        if (!xpath.isEmpty()) {
                            xpath.pop();
                        }
                    }

                    break;
                default:
                    //do nothing
                    break;
            }
            eventType = xmlStreamReader.next();
        }

        DBUtils.safeClose(xmlInputStream);
        DBUtils.safeClose(INSERT_NODES);
        DBUtils.safeClose(INSERT_RELATION_MEMBER);
        DBUtils.safeClose(INSERT_RELATIONS);
        DBUtils.safeClose(INSERT_TAG);
        DBUtils.safeClose(INSERT_WAYS);
        System.out.println("Total import time - " + (System.currentTimeMillis() - start) + "ms, total elements processed " + recordCount + " inserts " + inserts + " stack " + xpath.size());
        isReading = false;
    }

    public String getParserName() {
        return "PullParser";
    }

    @Override
    public void setOptions(Set<Short> options) {

        this.options.clear();
        this.options.addAll(options);
    }


}
