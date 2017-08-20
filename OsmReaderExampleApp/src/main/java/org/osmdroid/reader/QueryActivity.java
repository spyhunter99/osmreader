package org.osmdroid.reader;

import android.app.Activity;
import android.os.Bundle;
import android.util.Log;
import android.util.StringBuilderPrinter;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import org.osmdroid.api.IGeoPoint;
import org.osmdroid.api.IMapView;
import org.osmdroid.config.Configuration;
import org.osmdroid.events.MapListener;
import org.osmdroid.events.ScrollEvent;
import org.osmdroid.events.ZoomEvent;
import org.osmdroid.reader.example.R;
import org.osmdroid.reader.model.SearchResults;
import org.osmdroid.util.BoundingBox;
import org.osmdroid.util.GeoPoint;
import org.osmdroid.views.MapView;
import org.osmdroid.views.overlay.FolderOverlay;
import org.osmdroid.views.overlay.Marker;
import org.osmdroid.views.overlay.Overlay;

import java.io.File;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class QueryActivity extends Activity implements View.OnClickListener {
    public static final DecimalFormat df = new DecimalFormat("#.000000");
    TextView textViewCurrentLocation;
    MapView map;
    EditText searchBox;
    QueryToolsAndroid query=null;
    Button search;
    FolderOverlay searchResults = new FolderOverlay();
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Configuration.getInstance().setUserAgentValue(getPackageName());
        setContentView(R.layout.activity_query);
        map = (MapView) findViewById(R.id.mapView);

        map.setTilesScaledToDpi(true);
        map.setMultiTouchControls(true);
        map.setUseDataConnection(true);
        map.setBuiltInZoomControls(true);
        map.setMinZoomLevel(3);
        map.getController().setZoom(3);
        search = (Button) findViewById(R.id.search_button);
        search.setOnClickListener(this);

        searchBox = (EditText) findViewById(R.id.search_text);

        query = new QueryToolsAndroid(new File("/sdcard/data/maryland-latest.osm.pbf.sqlite"));
        map.getOverlayManager().add(searchResults);
        textViewCurrentLocation= (TextView) findViewById(R.id.currentLocation);
        map.setMapListener(new MapListener() {
            @Override
            public boolean onScroll(ScrollEvent event) {
                Log.i(IMapView.LOGTAG, System.currentTimeMillis() + " onScroll " + event.getX() + "," +event.getY() );
                //Toast.makeText(getActivity(), "onScroll", Toast.LENGTH_SHORT).show();
                updateInfo();
                return true;
            }

            @Override
            public boolean onZoom(ZoomEvent event) {
                Log.i(IMapView.LOGTAG, System.currentTimeMillis() + " onZoom " + event.getZoomLevel());
                updateInfo();
                return true;
            }
        });
    }

    private void updateInfo(){
        IGeoPoint mapCenter = map.getMapCenter();
        textViewCurrentLocation.setText(df.format(mapCenter.getLatitude())+","+
            df.format(mapCenter.getLongitude())
            +",zoom="+map.getZoomLevel());
       // triggerSearch();
    }

    private void triggerSearch(){
        String text = searchBox.getText().toString();
        if (text.length()>0) {
            searchResults.closeAllInfoWindows();
            searchResults.getItems().clear();
            BoundingBox boundingBox = map.getBoundingBox();
            try {
                List<SearchResults> search = query.search2(text, 1000, 0, boundingBox.getLatNorth(), boundingBox.getLonEast(), boundingBox.getLatSouth(), boundingBox.getLonWest());
                Iterator<SearchResults> iterator = search.iterator();
                while (iterator.hasNext()){
                    SearchResults next = iterator.next();
                    Map<String, String> tags = query.getTags(next.getDatabaseId(), 20, 0);

                    searchResults.add(toMarker(next,tags));
                    map.invalidate();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
    @Override
    public void onClick(View v) {
        triggerSearch();
    }

    private Marker toMarker(SearchResults next, Map<String, String> tags) {
        Marker m = new Marker(map);
        m.setPosition(new GeoPoint(next.getLat(), next.getLon()));
        m.setTitle(next.getName());
        Iterator<Map.Entry<String, String>> iterator = tags.entrySet().iterator();
        StringBuilder sb = new StringBuilder();
        while (iterator.hasNext()) {
            Map.Entry<String, String> next1 = iterator.next();
            sb.append(next1.getKey() + " = " + next1.getValue() + "<br>");
        }
        m.setSnippet(sb.toString());


        return m;
    }
}
