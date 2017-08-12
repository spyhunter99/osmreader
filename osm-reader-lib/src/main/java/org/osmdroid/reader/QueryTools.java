package org.osmdroid.reader;

import org.osmdroid.reader.model.OsmType;
import org.osmdroid.reader.model.SearchResults;
import org.osmdroid.reader.readers.OsmPullParserReader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Query Tools for .osm.bz2 in sqlite format
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 * @see OsmPullParserReader
 */

public class QueryTools {

    //TODO it would be helpful to limit the search results to a bounding box

    /**
     * searches for a set of key words and returns a search result set. The database connection
     * remains open after this call.
     * @param searchQuery
     * @param limit
     * @param offset
     * @param con
     * @return
     * @throws Exception
     */
    public static List<SearchResults> reverseGeocode(String searchQuery, int limit, int offset, Connection con) throws Exception {
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
                        }catch (Exception ex) {
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2=null;
                            cmd2=null;
                        }
                        rs2=null;
                        cmd2=null;

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
                                ret.add(loc);
                            }
                        }catch (Exception ex) {
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2=null;
                            cmd2=null;
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
                                ret.add(loc);
                            }
                        }catch (Exception ex){
                            throw ex;
                        } finally {
                            DBUtils.safeClose(rs2);
                            DBUtils.safeClose(cmd2);
                            rs2=null;
                            cmd2=null;
                        }


                        break;
                    case RELATION:
                        //NOOP
                        break;
                    default:
                        throw new IllegalArgumentException(type + " not supported");
                }
            }
        }catch (Exception ex) {
            throw ex;
        } finally {
            DBUtils.safeClose(rs);
            DBUtils.safeClose(rs2);
            DBUtils.safeClose(cmd);
            DBUtils.safeClose(cmd2);
            rs2=null;
            cmd2=null;
            rs=null;
            cmd=null;
        }
        return ret;
    }
}
