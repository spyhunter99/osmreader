package org.osmdroid.reader;

import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;

import org.osmdroid.reader.model.Address;
import org.osmdroid.reader.model.OsmType;
import org.osmdroid.reader.model.SearchResults;
import org.osmdroid.reader.readers.OsmPullParserReader;

import java.io.File;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Query Tools for .osm.bz2 in sqlite format
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 * @see OsmPullParserReader
 */

public class QueryToolsAndroid {

    SQLiteDatabase db=null;
    public QueryToolsAndroid(File database) {
        db = SQLiteDatabase.openOrCreateDatabase(database, null);
    }
    //TODO it would be helpful to limit the search results to a bounding box

    public static List<SearchResults> search(String searchQueryOptional, int limit, int offset, double lat, double lon, double radiusDistanceInMeters) {

        throw new IllegalArgumentException("no supported yet");
    }

    /**
     * returns (if available) the street or mailing address of the given entity, usually a node
     *
     * @param databaseId
     * @return
     * @throws Exception
     */
    public Address getAddress(long databaseId) throws Exception {

        Address r = new Address();

        Cursor tag=null;
        try {
             tag = db.query("tag", new String[]{"k,v"}, "id=?", new String[]{databaseId + ""}, null, null, null, null);
            while (tag.moveToNext()) {


                if ("phone".equalsIgnoreCase(tag.getString(0))) {
                    r.setPhone(tag.getString(1));
                } else if ("contact:phone".equalsIgnoreCase(tag.getString(0))) {
                    r.setPhone(tag.getString(1));
                } else if ("website".equalsIgnoreCase(tag.getString(0))) {
                    r.setWebsite(tag.getString(1));
                } else if ("contact:website".equalsIgnoreCase(tag.getString(0))) {
                    r.setWebsite(tag.getString(1));
                } else if ("note:website".equalsIgnoreCase(tag.getString(0))) {
                    r.setWebsite(tag.getString(1));
                } else if ("incorporation:website".equalsIgnoreCase(tag.getString(0))) {
                    r.setWebsite(tag.getString(1));
                } else if ("addr:city".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_city(tag.getString(1));
                } else if ("addr:state".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_state(tag.getString(1));
                } else if ("addr:housenumber".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_housenumber(tag.getString(1));
                } else if ("source:addr:housenumber".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_housenumber(tag.getString(1));
                } else if ("addr:housename".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_housename(tag.getString(1));
                } else if ("addr:postcode".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_postcode(tag.getString(1));
                } else if ("gnis:county_name".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_county(tag.getString(1));
                } else if ("addr:county".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_county(tag.getString(1));
                } else if ("addr:street".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_street(tag.getString(1));
                } else if ("street:addr".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_street(tag.getString(1));
                } else if ("addr:unit".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_unit(tag.getString(1));
                } else if ("addr:street_1".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_street_1(tag.getString(1));
                } else if ("addr:street_2".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_street_2(tag.getString(1));
                } else if ("addr:street_3".equalsIgnoreCase(tag.getString(0))) {
                    r.setAddr_street_3(tag.getString(1));
                }


            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            if (tag!=null)
            tag.close();
                    }
        return r;
    }

    /**
     * gets all tags for a given database record number
     *
     * @param databaseId
     * @return
     */
    public Map<String, String> getTags(long databaseId,  int limit, int offset) throws Exception {
        Map<String, String> ret = new HashMap<String, String>();
        Cursor rs = null;

        try {
            rs = db.query("tag", new String[]{"k,v"},"id=?",new String[]{databaseId+""},null,null,null,offset+","+limit);

            while (rs.moveToNext()){

                ret.put(rs.getString(0), rs.getString(1));
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            rs.close();
        }
        return ret;
    }

    /**
     * for bounding box queries, useful for on searching on screen
     *
     * @param searchQueryOptional
     * @param limit
     * @param offset
     * @param maxLat
     * @param maxLon
     * @param minLat
     * @param minLon
     * @return
     */
    public List<SearchResults> search(String searchQueryOptional, int limit, int offset, double maxLat, double maxLon, double minLat, double minLon) throws Exception {
        List<SearchResults> ret = new ArrayList<SearchResults>();

        Cursor rs = null;


        try {

            if (searchQueryOptional != null) {
                rs = db.rawQuery("SELECT * FROM tag inner join nodes on tag.id=nodes.id where k='name' and v like ? and lat > ? and lat < ? and lon >? and lon < ? limit ? offset ?",
                    new String[]{"%" + searchQueryOptional + "%",minLat+"", + maxLat +"", minLon+"", maxLon+"", limit+"",offset+""});

            } else {
                rs = db.rawQuery("SELECT * FROM tag inner join nodes on tag.id=nodes.id where k='name' and lat > ? and lat < ? and lon >? and lon < ? limit ? offset ?",
                    new String[]{minLat+"", + maxLat +"", minLon+"", maxLon+"", limit+"",offset+""});
            }

            while (rs.moveToNext()) {
                int type = rs.getInt(rs.getColumnIndex("reftype"));
                OsmType rowType = OsmType.values()[type];
                SearchResults loc = new SearchResults();
                loc.setLat(rs.getDouble(rs.getColumnIndex("lat")));
                loc.setLon(rs.getDouble(rs.getColumnIndex("lon")));
                loc.setName(rs.getString(rs.getColumnIndex("v")));
                loc.setType(rowType);
                loc.setDatabaseId(rs.getLong(rs.getColumnIndex("id")));
                ret.add(loc);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            rs.close();
        }
        return ret;

    }

    /**
     * searches for a set of key words and returns a search result set. The database connection
     * remains open after this call.
     *
     * @param searchQuery
     * @param limit
     * @param offset
     * @return
     * @throws Exception
     */
    public List<SearchResults> search2(String searchQuery, int limit, int offset, double maxLat, double maxLon, double minLat, double minLon) throws Exception {
        List<SearchResults> ret = new ArrayList<SearchResults>();

        Cursor rs = null;
        Cursor  rs2 = null;


        try {
            rs = db.rawQuery("SELECT * FROM tag where k='name' and v like ? limit ? offset ?",
                new String[]{"%" + searchQuery + "%", limit+"", offset+""});


            while (rs.moveToNext()) {
                int type = rs.getInt(rs.getColumnIndex("reftype"));
                OsmType rowType = OsmType.values()[type];

                switch (rowType) {
                    case WAY:
                        //some kind of road.
                        //get the nodes
                        int nodeid = -1;
                        try {
                            //this will get the node id
                            rs2 = db.rawQuery("select node_id from way_no where way_id=? limit 1", new String[]{rs.getInt(rs.getColumnIndex("id"))+""});
                            if (rs2.moveToNext()) {
                                nodeid = rs2.getInt(0);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            rs2.close();
                        }
                        rs2 = null;

                        try {
                            rs2 = db.rawQuery("select lat,lon from nodes where id=? and lat > ? and lat < ? and lon >? and lon < ? limit 1",
                                new String[]{nodeid+"",minLat+"", + maxLat +"", minLon+"", maxLon+""});

                            //get the first lat lon and use that as the point


                            if (rs2.moveToNext()) {
                                SearchResults loc = new SearchResults();
                                loc.setLat(rs2.getDouble(rs2.getColumnIndex("lat")));
                                loc.setLon(rs2.getDouble(rs2.getColumnIndex("lon")));
                                loc.setName(rs.getString(rs.getColumnIndex("v")));
                                loc.setType(rowType);
                                loc.setDatabaseId(nodeid);
                                ret.add(loc);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            rs2.close();
                        }

                        break;

                    case NODE:
                        //town city, poi
                        try {
                            rs2=db.rawQuery("select lat,lon from nodes where id=?  and lat > ? and lat < ? and lon >? and lon < ? limit 1",
                                new String[]{rs.getInt(rs.getColumnIndex("id"))+"",minLat+"", + maxLat +"", minLon+"", maxLon+""});

                            //this will get the node text

                            if (rs2.moveToNext()) {
                                SearchResults loc = new SearchResults();
                                loc.setLat(rs2.getDouble(rs2.getColumnIndex("lat")));
                                loc.setLon(rs2.getDouble(rs2.getColumnIndex("lon")));
                                loc.setName(rs.getString(rs.getColumnIndex("v")));
                                loc.setType(rowType);
                                loc.setDatabaseId(rs.getLong(rs.getColumnIndex("id")));
                                ret.add(loc);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            rs2.close();
                        }


                        break;
                    case RELATION:
                        //NOOP
                        break;
                    default:
                        //throw new IllegalArgumentException(type + " not supported");
                        break;
                }
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            rs.close();
            if (rs2!=null && !rs2.isClosed())
            rs2.close();
        }
        return ret;
    }
}
