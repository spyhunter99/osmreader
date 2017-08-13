package org.osmdroid.reader.model;

import org.osmdroid.reader.model.OsmType;

/**
 * created on 8/12/2017.
 *
 * @author Alex O'Ree
 */
public class SearchResults {
    private double lat;
    private double lon;
    private String name;
    private OsmType type;
    private long databaseId;

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
        this.lon = lon;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public OsmType getType() {
        return type;
    }

    public void setType(OsmType type) {
        this.type = type;
    }

    public long getDatabaseId() {
        return databaseId;
    }

    public void setDatabaseId(long databaseId) {
        this.databaseId = databaseId;
    }
}
