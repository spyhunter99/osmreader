package org.osmdroid.reader.readers;

import org.openstreetmap.osmosis.core.container.v0_6.EntityContainer;
import org.openstreetmap.osmosis.core.domain.v0_6.Entity;
import org.openstreetmap.osmosis.core.domain.v0_6.EntityType;
import org.openstreetmap.osmosis.core.domain.v0_6.Node;
import org.openstreetmap.osmosis.core.domain.v0_6.Relation;
import org.openstreetmap.osmosis.core.domain.v0_6.RelationMember;
import org.openstreetmap.osmosis.core.domain.v0_6.Tag;
import org.openstreetmap.osmosis.core.domain.v0_6.Way;
import org.openstreetmap.osmosis.core.domain.v0_6.WayNode;
import org.openstreetmap.osmosis.core.task.v0_6.RunnableSource;
import org.openstreetmap.osmosis.core.task.v0_6.Sink;
import org.openstreetmap.osmosis.xml.common.CompressionMethod;
import org.openstreetmap.osmosis.xml.v0_6.XmlReader;
import org.osmdroid.reader.DBUtils;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.model.OsmType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class OsmosisReader implements IOsmReader, Sink {
    private static final Logger LOG = Logger.getLogger(OsmosisReader.class.getName());

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


    /**
     * gets at estimate for completion.
     *
     * @return
     */
    @Override
    public double getProgress() {
        if (isReading) {
            //delaware has 11000000 bytes
            //with 6119693 xml elements
            long expectedRecordCount = (long) (double) ((double) inputFileSize * 6119693d / 11000000d);

            double value = (((double) recordCount) / expectedRecordCount) * 100d;

            return value;

        }
        return -1;
    }


    private boolean isReading = false;
    private long recordCount = 0;
    private long inputFileSize = 0;
    private Set<Short> options = new HashSet<Short>();

    boolean hasNodes = false;
    boolean hasRelationMembers = false;
    boolean hasRelations = false;
    boolean hasTags = false;
    boolean hasWayNo = false;
    boolean hasWays = false;
    long inserts = 0;


    PreparedStatement INSERT_NODES;

    PreparedStatement INSERT_RELATIONS;

    PreparedStatement INSERT_WAYS;

    PreparedStatement INSERT_TAG;

    PreparedStatement INSERT_WAY_NO;

    PreparedStatement INSERT_RELATION_MEMBER;

    Connection connection;
    long batchCount = 0;
    final int BATCH_SIZE = 100;


    @Override
    public synchronized void read(File path, Connection connection) throws Exception {
        File file = path; // the input file

        long start = System.currentTimeMillis();
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
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"relations\" (\"id\" INTEGER PRIMARY KEY  NOT NULL , \"user\" TEXT, \"uid\" INTEGER, \"version\" INTEGER, \"changeset\" INTEGER, \"timestamp\" BIGINT)");
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
            p = connection.prepareStatement("CREATE TABLE IF NOT EXISTS  \"ways\" (\"id\" INTEGER PRIMARY KEY  NOT NULL , \"changeset\" INTEGER, \"version\" INTEGER, \"user\" TEXT, \"uid\" INTEGER, \"timestamp\" BIGINT)");
            p.execute();
            p.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        INSERT_NODES = connection.prepareStatement("INSERT OR REPLACE INTO nodes (id,changeset,version,user,uid,timestamp,lat,lon) "
            + "VALUES (?,?,?,?, ?,?,?,?); ");
        connection.setAutoCommit(false);

        INSERT_RELATIONS = connection.prepareStatement("INSERT OR REPLACE INTO relations (id,changeset,version,user,uid,timestamp) "
            + "VALUES (?,?,?,?,?,?); ");

        INSERT_WAYS = connection.prepareStatement("INSERT OR REPLACE INTO ways (id,changeset,version,user,uid,timestamp) "
            + "VALUES (?,?,?,?,?,?); ");

        INSERT_TAG = connection.prepareStatement("INSERT OR REPLACE INTO tag (id,k,v,reftype) "
            + "VALUES (?,?,?,?); ");

        INSERT_WAY_NO = connection.prepareStatement("INSERT OR REPLACE INTO way_no (way_id,node_id) "
            + "VALUES (?,?); ");

        INSERT_RELATION_MEMBER = connection.prepareStatement("INSERT OR REPLACE INTO relation_members (id,type,ref,role) "
            + "VALUES (?,?,?,?); ");

        this.connection = connection;


        connection.setAutoCommit(false);


        isReading = true;

        Date timestamp = new Date(System.currentTimeMillis());


        boolean pbf = false;
        CompressionMethod compression = CompressionMethod.None;

        if (file.getName().endsWith(".pbf")) {
            pbf = true;
        } else if (file.getName().endsWith(".gz")) {
            compression = CompressionMethod.GZip;
        } else if (file.getName().endsWith(".bz2")) {
            compression = CompressionMethod.BZip2;
        }

        RunnableSource reader;
        FileInputStream fis = null;
        if (pbf) {
            fis = new FileInputStream(file);
            reader = new crosby.binary.osmosis.OsmosisReader(
                fis);
        } else {
            reader = new XmlReader(file, false, compression);
        }

        reader.setSink(this);

        Thread readerThread = new Thread(reader);
        readerThread.start();

        while (readerThread.isAlive()) {
            try {
                readerThread.join();
            } catch (InterruptedException e) {
        /* do nothing */
            }
        }


        checkCommit(true);


        DBUtils.safeClose(fis);
        DBUtils.safeClose(INSERT_NODES);
        DBUtils.safeClose(INSERT_RELATION_MEMBER);
        DBUtils.safeClose(INSERT_RELATIONS);
        DBUtils.safeClose(INSERT_TAG);
        DBUtils.safeClose(INSERT_WAYS);

        INSERT_NODES = null;
        INSERT_RELATION_MEMBER = null;
        INSERT_RELATIONS = null;
        INSERT_TAG = null;
        INSERT_WAYS = null;
        this.connection = null;
        System.out.println("Total import time - " + (System.currentTimeMillis() - start) + "ms, total elements processed " + recordCount + " inserts " + inserts);
        isReading = false;

    }

    private void checkCommit(boolean force) {
        if (force || (batchCount > BATCH_SIZE)) {
            batchCount = 0;
            try {
                if (hasNodes) {
                    try {
                        INSERT_NODES.executeBatch();

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_NODES.clearBatch();
                    }
                }
                if (hasRelationMembers) {
                    try {
                        INSERT_RELATION_MEMBER.executeBatch();

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_RELATION_MEMBER.clearBatch();

                    }
                }
                if (hasRelations) {
                    try {
                        INSERT_RELATIONS.executeBatch();
                        INSERT_RELATIONS.clearBatch();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_RELATIONS.clearBatch();
                    }
                }
                if (hasTags) {
                    try {
                        INSERT_TAG.executeBatch();
                        INSERT_TAG.clearBatch();
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_TAG.clearBatch();
                    }
                }
                if (hasWayNo) {
                    try {
                        INSERT_WAY_NO.executeBatch();

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_WAY_NO.clearBatch();
                    }
                }
                if (hasWays) {
                    try {

                        INSERT_WAYS.executeBatch();

                    } catch (Exception ex) {
                        ex.printStackTrace();
                    } finally {
                        INSERT_WAYS.clearBatch();
                    }
                }
                connection.commit();

                hasNodes = false;
                hasRelationMembers = false;
                hasRelations = false;
                hasTags = false;
                hasWayNo = false;
                hasWays = false;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }


    @Override
    public String getParserName() {
        return "Osmosis";
    }

    @Override
    public void setOptions(Set<Short> options) {

        this.options.clear();
        this.options.addAll(options);
    }

    @Override
    public void process(EntityContainer entityContainer) {
        Entity entity = entityContainer.getEntity();
        if (entity instanceof Node) {
            //do something with the node
            Node n = (Node) entity;
            if (!n.getTags().isEmpty()) {
                Iterator<Tag> iterator = n.getTags().iterator();
                while (iterator.hasNext()) {
                    Tag next = iterator.next();
                    try {
                        INSERT_TAG.setLong(1, n.getId());
                        INSERT_TAG.setString(2, next.getKey());
                        INSERT_TAG.setString(3, next.getValue());
                        INSERT_TAG.setInt(4, translate(n.getType()));
                        INSERT_TAG.addBatch();
                        hasTags = true;
                        inserts++;
                        batchCount++;
                    } catch (Exception ex) {
                        System.out.println(INSERT_TAG.toString());
                        ex.printStackTrace();
                        //INSERT_TAG.clearWarnings();
                    }
                }
            }
            try {
                INSERT_NODES.setLong(1, n.getId());
                INSERT_NODES.setLong(2, n.getChangesetId());
                INSERT_NODES.setLong(3, n.getVersion());
                INSERT_NODES.setString(4, n.getUser().getName());
                INSERT_NODES.setLong(5, n.getUser().getId());
                INSERT_NODES.setLong(6, n.getTimestamp().getTime());
                INSERT_NODES.setDouble(7, n.getLatitude());
                INSERT_NODES.setDouble(8, n.getLongitude());
                INSERT_NODES.addBatch();
                hasNodes = true;
                inserts++;
                batchCount++;
            } catch (Exception ex) {
                System.out.println(INSERT_NODES.toString());
                ex.printStackTrace();
            }
        } else if (entity instanceof Way) {
            //do something with the way
            if (options.contains(ImportOptions.INCLUDE_WAYS)) {

                Way n = (Way) entity;
                if (!n.getTags().isEmpty()) {
                    Iterator<Tag> iterator = n.getTags().iterator();
                    while (iterator.hasNext()) {
                        Tag next = iterator.next();
                        try {
                            INSERT_TAG.setLong(1, n.getId());
                            INSERT_TAG.setString(2, next.getKey());
                            INSERT_TAG.setString(3, next.getValue());
                            INSERT_TAG.setInt(4, translate(n.getType()));
                            INSERT_TAG.addBatch();
                            hasTags = true;
                            inserts++;
                            batchCount++;
                        } catch (Exception ex) {
                            System.out.println(INSERT_TAG.toString());
                            ex.printStackTrace();
                            //INSERT_TAG.clearWarnings();
                        }
                    }
                }
                try {
                    INSERT_WAYS.setLong(1, n.getId());
                    INSERT_WAYS.setLong(2, n.getChangesetId());
                    INSERT_WAYS.setLong(3, n.getVersion());
                    INSERT_WAYS.setString(4, n.getUser().getName());
                    INSERT_WAYS.setLong(5, n.getUser().getId());
                    INSERT_WAYS.setLong(6, n.getTimestamp().getTime());
                    INSERT_WAYS.addBatch();
                    hasWays = true;
                    inserts++;
                    batchCount++;
                } catch (Exception ex) {
                    System.out.println(INSERT_WAYS.toString());
                    ex.printStackTrace();

                }


                if (n.getWayNodes() != null) {
                    Iterator<WayNode> iterator = n.getWayNodes().iterator();
                    while (iterator.hasNext()) {
                        WayNode next = iterator.next();

                        try {
                            INSERT_WAY_NO.setLong(1, n.getId());
                            INSERT_WAY_NO.setLong(2, next.getNodeId());
                            INSERT_WAY_NO.addBatch();
                            batchCount++;
                            hasWayNo = true;
                            inserts++;
                        } catch (Exception ex) {
                            System.out.println(INSERT_WAY_NO.toString());
                            ex.printStackTrace();
                        }
                    }
                }
            }

        } else if (entity instanceof Relation) {
            if (options.contains(ImportOptions.INCLUDE_RELATIONS)) {
                //do something with the relation
                Relation n = (Relation) entity;
                if (!n.getTags().isEmpty()) {
                    Iterator<Tag> iterator = n.getTags().iterator();
                    while (iterator.hasNext()) {
                        Tag next = iterator.next();
                        try {
                            INSERT_TAG.setLong(1, n.getId());
                            INSERT_TAG.setString(2, next.getKey());
                            INSERT_TAG.setString(3, next.getValue());
                            INSERT_TAG.setInt(4, translate(n.getType()));
                            INSERT_TAG.addBatch();
                            hasTags = true;
                            inserts++;
                            batchCount++;
                        } catch (Exception ex) {
                            System.out.println(INSERT_TAG.toString());
                            ex.printStackTrace();
                            //INSERT_TAG.clearWarnings();
                        }
                    }
                }
                try {

                    INSERT_RELATIONS.setLong(1, n.getId());
                    INSERT_RELATIONS.setLong(2, n.getChangesetId());
                    INSERT_RELATIONS.setLong(3, n.getVersion());
                    INSERT_RELATIONS.setString(4, n.getUser().getName());
                    INSERT_RELATIONS.setLong(5, n.getUser().getId());
                    INSERT_RELATIONS.setLong(6, n.getTimestamp().getTime());
                    INSERT_RELATIONS.addBatch();
                    hasRelations = true;
                    batchCount++;
                    inserts++;
                } catch (Exception ex) {
                    System.out.println(INSERT_RELATIONS.toString());
                    ex.printStackTrace();

                }

                if (n.getMembers() != null) {
                    Iterator<RelationMember> iterator = n.getMembers().iterator();
                    while (iterator.hasNext()) {
                        RelationMember next = iterator.next();
                        try {
                            //(id,type,ref,role)
                            INSERT_RELATION_MEMBER.setLong(1, n.getId());
                            INSERT_RELATION_MEMBER.setString(2, next.getMemberType().name());
                            INSERT_RELATION_MEMBER.setLong(3, next.getMemberId());
                            INSERT_RELATION_MEMBER.setString(4, next.getMemberRole());
                            INSERT_RELATION_MEMBER.addBatch();
                            hasRelationMembers = true;
                            inserts++;
                            batchCount++;
                        } catch (Exception ex) {
                            System.out.println(INSERT_RELATION_MEMBER.toString());
                            ex.printStackTrace();

                        }
                    }
                }
            }
        }
        checkCommit(false);
    }

    private int translate(EntityType type) {
        switch (type) {
            case Bound:
                return OsmType.BOUND.ordinal();
            case Node:
                return OsmType.NODE.ordinal();
            case Relation:
                return OsmType.RELATION.ordinal();
            case Way:
                return OsmType.WAY.ordinal();
        }
        return -1;
    }

    @Override
    public void initialize(Map<String, Object> metaData) {

    }

    @Override
    public void complete() {

    }

    @Override
    public void release() {

    }
}
