package org.osmdroid.reader;

import org.osmdroid.reader.model.Address;
import org.osmdroid.reader.model.OsmType;
import org.osmdroid.reader.model.SearchResults;
import org.osmdroid.reader.readers.OsmPullParserReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

public class QueryTools {

    //TODO it would be helpful to limit the search results to a bounding box

    public static List<SearchResults> search(String searchQueryOptional, int limit, int offset, Connection con, double lat, double lon, double radiusDistanceInMeters) {

        throw new IllegalArgumentException("no supported yet");
    }

    /**
     * returns (if available) the street or mailing address of the given entity, usually a node
     *
     * @param databaseId
     * @param con
     * @return
     * @throws Exception
     */
    public static Address getAddress(long databaseId, Connection con) throws Exception {
        ResultSet rs = null;
        Address r = new Address();

        PreparedStatement cmd = null;
        try {
            cmd = con.prepareStatement("SELECT k,v FROM tag where id=?");
            cmd.setLong(1, databaseId);

            rs = cmd.executeQuery();
            while (rs.next()) {
                if ("phone".equalsIgnoreCase(rs.getString("k"))) {
                    r.setPhone(rs.getString("v"));
                } else if ("contact:phone".equalsIgnoreCase(rs.getString("k"))) {
                    r.setPhone(rs.getString("v"));
                } else if ("website".equalsIgnoreCase(rs.getString("k"))) {
                    r.setWebsite(rs.getString("v"));
                } else if ("contact:website".equalsIgnoreCase(rs.getString("k"))) {
                    r.setWebsite(rs.getString("v"));
                } else if ("note:website".equalsIgnoreCase(rs.getString("k"))) {
                    r.setWebsite(rs.getString("v"));
                } else if ("incorporation:website".equalsIgnoreCase(rs.getString("k"))) {
                    r.setWebsite(rs.getString("v"));
                } else if ("addr:city".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_city(rs.getString("v"));
                } else if ("addr:state".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_state(rs.getString("v"));
                } else if ("addr:housenumber".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_housenumber(rs.getString("v"));
                } else if ("source:addr:housenumber".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_housenumber(rs.getString("v"));
                } else if ("addr:housename".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_housename(rs.getString("v"));
                } else if ("addr:postcode".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_postcode(rs.getString("v"));
                } else if ("gnis:county_name".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_county(rs.getString("v"));
                } else if ("addr:county".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_county(rs.getString("v"));
                } else if ("addr:street".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_street(rs.getString("v"));
                } else if ("street:addr".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_street(rs.getString("v"));
                } else if ("addr:unit".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_unit(rs.getString("v"));
                } else if ("addr:street_1".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_street_1(rs.getString("v"));
                } else if ("addr:street_2".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_street_2(rs.getString("v"));
                } else if ("addr:street_3".equalsIgnoreCase(rs.getString("k"))) {
                    r.setAddr_street_3(rs.getString("v"));
                }


            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            DBUtils.safeClose(rs);
            DBUtils.safeClose(cmd);
            rs = null;
            cmd = null;
        }
        return r;
    }

    /**
     * gets all tags for a given database record number
     *
     * @param databaseId
     * @return
     */
    public static Map<String, String> getTags(long databaseId, Connection con, int limit, int offset) throws Exception {
        Map<String, String> ret = new HashMap<String, String>();
        ResultSet rs = null;

        PreparedStatement cmd = null;
        try {
            cmd = con.prepareStatement("SELECT k,v FROM tag where id=? limit ? offset ?");
            cmd.setLong(1, databaseId);
            cmd.setInt(2, limit);
            cmd.setInt(3, offset);
            rs = cmd.executeQuery();
            while (rs.next()) {
                ret.put(rs.getString("k"), rs.getString("v"));
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            DBUtils.safeClose(rs);
            DBUtils.safeClose(cmd);
            rs = null;
            cmd = null;
        }
        return ret;
    }

    /**
     * for bounding box queries, useful for on searching on screen
     *
     * @param searchQueryOptional
     * @param limit
     * @param offset
     * @param con
     * @param maxLat
     * @param maxLon
     * @param minLat
     * @param minLon
     * @return
     */
    public static List<SearchResults> search(String searchQueryOptional, int limit, int offset, Connection con, double maxLat, double maxLon, double minLat, double minLon) throws Exception {
        List<SearchResults> ret = new ArrayList<SearchResults>();

        ResultSet rs = null;

        PreparedStatement cmd = null;


        try {

            if (searchQueryOptional != null) {
                cmd = con.prepareStatement("SELECT * FROM tag inner join nodes on tag.id=nodes.id where k='name' and v like ? and lat > ? and lat < ? and lon >? and lon < ? limit ? offset ?");
                cmd.setString(1, "%" + searchQueryOptional + "%");
                cmd.setDouble(2, minLat);
                cmd.setDouble(3, maxLat);
                cmd.setDouble(4, minLon);
                cmd.setDouble(5, maxLon);
                cmd.setInt(6, limit);
                cmd.setInt(7, offset);

            } else {
                cmd = con.prepareStatement("SELECT * FROM tag inner join nodes on tag.id=nodes.id where k='name' and lat > ? and lat < ? and lon >? and lon < ? limit ? offset ?");
                cmd.setDouble(1, minLat);
                cmd.setDouble(2, maxLat);
                cmd.setDouble(3, minLon);
                cmd.setDouble(4, maxLon);
                cmd.setInt(5, limit);
                cmd.setInt(6, offset);
            }

            rs = cmd.executeQuery();
            while (rs.next()) {
                int type = rs.getInt("reftype");
                OsmType rowType = OsmType.values()[type];
                SearchResults loc = new SearchResults();
                loc.setLat(rs.getDouble("lat"));
                loc.setLon(rs.getDouble("lon"));
                loc.setName(rs.getString("v"));
                loc.setType(rowType);
                loc.setDatabaseId(rs.getLong("id"));
                ret.add(loc);
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            DBUtils.safeClose(rs);
            DBUtils.safeClose(cmd);
            rs = null;
            cmd = null;
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
     * @param con
     * @return
     * @throws Exception
     */
    public static List<SearchResults> search(String searchQuery, int limit, int offset, Connection con) throws Exception {
        List<SearchResults> ret = new ArrayList<SearchResults>();

        ResultSet rs = null;
        ResultSet rs2 = null;
        PreparedStatement cmd = null;
        PreparedStatement cmd2 = null;

        try {
            cmd = con.prepareStatement("SELECT * FROM tag where k='name' and v like ? limit ? offset ?");
            cmd.setString(1, "%" + searchQuery + "%");
            cmd.setInt(2, limit);
            cmd.setInt(3, offset);

            rs = cmd.executeQuery();


            while (rs.next()) {
                int type = rs.getInt("reftype");
                OsmType rowType = OsmType.values()[type];

                switch (rowType) {
                    case WAY:
                        //some kind of road.
                        //get the nodes
                        int nodeid = -1;
                        try {
                            cmd2 = con.prepareStatement("select node_id from way_no where way_id=? limit 1");
                            cmd2.setInt(1, rs.getInt("id"));    //TODO verify
                            //this will get the node id

                            rs2 = cmd2.executeQuery();
                            if (rs2.next()) {
                                nodeid = rs2.getInt(0);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2 = null;
                            cmd2 = null;
                        }
                        rs2 = null;
                        cmd2 = null;

                        try {
                            cmd2 = con.prepareStatement("select lat,lon from nodes where id=? limit 1");
                            cmd2.setInt(1, nodeid);    //TODO verify
                            //get the first lat lon and use that as the point

                            rs2 = cmd2.executeQuery();
                            if (rs2.next()) {
                                SearchResults loc = new SearchResults();
                                loc.setLat(rs2.getDouble("lat"));
                                loc.setLon(rs2.getDouble("lon"));
                                loc.setName(rs.getString("v"));
                                loc.setType(rowType);
                                loc.setDatabaseId(rs.getLong("id"));
                                ret.add(loc);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2 = null;
                            cmd2 = null;
                        }

                        break;

                    case NODE:
                        //town city, poi
                        try {
                            cmd2 = con.prepareStatement("select lat,lon from nodes where id=? limit 1");
                            cmd2.setInt(1, rs.getInt("id"));    //TODO verify
                            //this will get the node text
                            rs2 = cmd2.executeQuery();
                            if (rs2.next()) {
                                SearchResults loc = new SearchResults();
                                loc.setLat(rs2.getDouble("lat"));
                                loc.setLon(rs2.getDouble("lon"));
                                loc.setName(rs.getString("v"));
                                loc.setType(rowType);
                                loc.setDatabaseId(rs.getLong("id"));
                                ret.add(loc);
                            }
                        } catch (Exception ex) {
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2 = null;
                            cmd2 = null;
                        }


                        break;
                    case RELATION:
                        //NOOP
                        break;
                    default:
                        throw new IllegalArgumentException(type + " not supported");
                }
            }
        } catch (Exception ex) {
            throw ex;
        } finally {
            DBUtils.safeClose(rs);
            DBUtils.safeClose(rs2);
            DBUtils.safeClose(cmd);
            DBUtils.safeClose(cmd2);
            rs2 = null;
            cmd2 = null;
            rs = null;
            cmd = null;
        }
        return ret;
    }
}
