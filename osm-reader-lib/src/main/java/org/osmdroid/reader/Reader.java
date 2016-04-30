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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Stack;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.stream.events.XMLEvent;

/**
 * to get a reverse geocode name to lat,lon 
 * SELECT * FROM tag where k='name' and v like '%new castle%'
 * 
 * get the refTag.
 * switch refTag
 * case WAY 0
 *   road of some sort
 *        get the nodes
 *             select node_id from way_no where way_id=?
 *                  select lat,lon from nodes where id=?
 * case NODE 1
 *   (towns, cities, POIs)
 *        select lat,lon from nodes where id=?
 * case RELATION 2
 
 * 
 * 
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


     static Connection connection;
     static PreparedStatement p;


     public void read(String path, Connection connection) throws Exception{

          //Connection connection = DriverManager.getConnection("jdbc:sqlite:/sdcard/osmdata3.sqlite");
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

//          System.console().readLine();

          BufferedReader xmlInputStream = getBufferedReaderForBZ2File(path);
          //new FileInputStream();
          XmlPullParserFactory factory = XmlPullParserFactory.newInstance();
          //factory.setNamespaceAware(true);
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
          String key = "";
          String val = "";


          int eventType=-1;
          Date timestamp = new Date(System.currentTimeMillis());
          //connection.setAutoCommit(false);
          for (eventType = xmlStreamReader.getEventType();
               eventType != XmlPullParser.END_DOCUMENT;
               eventType = xmlStreamReader.next()){
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

               //long btime = System.currentTimeMillis();
               switch (eventType) {
                    case XMLEvent.START_ELEMENT:

                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("bounds")) {
                              continue;
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
                         } else
                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("relation")) {
                              xpath.push(xmlStreamReader.getName().toString());
                              p = connection.prepareStatement("INSERT INTO relations (id,changeset,version,user,uid,timestamp) "
                                      + "VALUES (?,?,?,?,?,?); ");

                              for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                   //		System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                   //			+ xmlStreamReader.getAttributeValue(i));
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("id")) {
                                        id = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                   } else
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("user")) {
                                        user = xmlStreamReader.getAttributeValue(i);
                                   }else
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("uid")) {
                                        uid = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                   }else
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("changeset")) {
                                        changset = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                   }else
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("version")) {
                                        version = Long.parseLong(xmlStreamReader.getAttributeValue(i));
                                   }else
                                   if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("timestamp")) {
                                        timestamp.setTime(sdf.parse(xmlStreamReader.getAttributeValue(i)).getTime());
                                   }else{
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
                                   } catch (Exception ex) {
                                        System.out.println(p.toString());
                                        ex.printStackTrace();
                                   }
                                   p.close();
                                   p = null;
                              }
                              lastId = id;
                              lastType = OsmType.RELATION;
                         } else
                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("way")) {
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

                         } else

                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("tag")) {
                              if (!xpath.isEmpty() && ((xpath.peek().equalsIgnoreCase("way") || xpath.peek().equalsIgnoreCase("node") || xpath.peek().equalsIgnoreCase("relation")) && lastId != -1)) {

                                   for (int i = 0; i < xmlStreamReader.getAttributeCount(); i++) {
                                        //System.out.println(xmlStreamReader.getAttributeName(i) + "="
                                        //	+ xmlStreamReader.getAttributeValue(i));
                                        if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("k")) {
                                             key = xmlStreamReader.getAttributeValue(i);
                                        } else
                                        if (xmlStreamReader.getAttributeName(i).equalsIgnoreCase("v")) {
                                             val = xmlStreamReader.getAttributeValue(i);
                                        } else {
                                             //uncaptured attribute
                                             System.out.println(xmlStreamReader.getAttributeName(i) + "="+xmlStreamReader.getAttributeValue(i));
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
                         }else
                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("nd")) {
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

                         }else

                         if (xmlStreamReader.getName().toString().equalsIgnoreCase("member")) {

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
                    case XMLEvent.CHARACTERS:
                         //System.out.print("text!" + xmlStreamReader.getText());
                         break;
                    case XMLEvent.END_ELEMENT:
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
               System.out.println(System.currentTimeMillis() + " commit! inserts, total record count " + recordCount + " stack " + xpath.size());

               //batchCount = 0;
               //System.gc();
               //System.out.println();
               //  }
          }

     }

     public static void main(String[] args) throws Exception {
          //2010-03-16T11:47:08Z

          //java.util.Date parse = sdf.parse("2010-03-16T11:47:08Z");
          //System.out.println(parse.toString());
          Connection connection = DriverManager.getConnection("jdbc:sqlite:osmdata3.sqlite");

          new Reader().read(args[0],connection);
     }

     @Override
     public void finalize() {
          try {
               super.finalize();
          } catch (Throwable ex) {
               Logger.getLogger(Reader.class.getName()).log(Level.SEVERE, null, ex);
          }
          if (connection != null) {
               try {
                    connection.close();
               } catch (SQLException ex) {
                    Logger.getLogger(Reader.class.getName()).log(Level.SEVERE, null, ex);
               }
          }

     }
}
