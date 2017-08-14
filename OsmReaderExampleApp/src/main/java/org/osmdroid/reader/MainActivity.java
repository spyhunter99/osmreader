package org.osmdroid.reader;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.ProgressBar;
import android.widget.TextView;
import android.widget.Toast;

import org.osmdroid.reader.example.R;
import org.osmdroid.reader.model.ImportOptions;
import org.osmdroid.reader.readers.IOsmReader;
import org.osmdroid.reader.readers.OsmPullParserReader;
import org.osmdroid.reader.readers.OsmReaderFactory;
import org.osmdroid.reader.readers.OsmosisReader;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

import static org.osmdroid.reader.Main.formatter;
import static org.osmdroid.reader.Main.toHumanReadableDuration;

//Select a POI database
//query for POIs
//reverse geocode
//download + import a file

public class MainActivity extends AppCompatActivity {
    boolean running = true;
    ProgressBar bar ;
    TextView status;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        status = (TextView) findViewById(R.id.importStatus);
        bar = (ProgressBar) findViewById(R.id.importProgress);

        //org.sqldroid.Log.LEVEL = android.util.Log.VERBOSE;


        //TODO this needs a file browser to list all bz2 files
        //then with user selection, start the import
        //probably should use a common database name and location to keep things simple
        //

        final IOsmReader iOsmReader = new OsmPullParserReader();
        final long start = System.currentTimeMillis();
        Set<Short> opts = new HashSet<Short>();
        opts.add(ImportOptions.INCLUDE_RELATIONS);
        opts.add(ImportOptions.INCLUDE_WAYS);
        iOsmReader.setOptions(opts);
        iOsmReader.setBatchSize(0);

        //this updates the UI
        new Thread(new Runnable() {
            @Override
            public void run() {
                System.out.println("poller started");
                System.out.println((System.currentTimeMillis() - start) + " status " + iOsmReader.getProgress() + "% complete");
                while (running) {
                    try {
                        long elapsedTime = (System.currentTimeMillis() - start);
                        final double percentDone =  iOsmReader.getProgress();
                        long totalEstimatedTimeMs = (long)((elapsedTime/percentDone) * (100-percentDone));
                        String readable = toHumanReadableDuration(totalEstimatedTimeMs);
                        final String msg = (elapsedTime + " status " + formatter.format(percentDone) + "% complete. Est time remaining: " + readable);
                        runOnUiThread(new Runnable() {
                            @Override
                            public void run() {
                                if (running) {
                                    bar.setProgress((int) percentDone);
                                    status.setText(msg);
                                }
                            }
                        });
                        Thread.sleep(1000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

        //this does the work
        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    //boolean android = org.sqlite.util.OSInfo.isAndroid();

                    try {
                        DriverManager.registerDriver((Driver) Class.forName("org.sqldroid.SQLDroidDriver").newInstance());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        DriverManager.registerDriver((Driver) Class.forName("org.sqlite.JDBC").newInstance());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    File db = new File("/sdcard/importTest.sqlite");
                    if (db.exists())
                        db.delete();

                    //, true,getClass().getClassLoader()).newInstance()));
                    //Connection con = DriverManager.getConnection("jdbc:sqldroid:/sdcard/importTest.sqlite");
                    Connection con = DriverManager.getConnection("jdbc:sqlite:/sdcard/importTest.sqlite");

                    final long now = System.currentTimeMillis();


                    iOsmReader.setBatchSize(400);
                    iOsmReader.read(new File("/sdcard/delaware-latest.osm.bz2"), con);
                    running=false;
                    DBUtils.safeClose(con);

                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            Toast.makeText(MainActivity.this, "success "+(System.currentTimeMillis()-now), Toast.LENGTH_LONG).show();
                        }
                    });

                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            status.setText("Import complete in "  + (System.currentTimeMillis()- now) + "ms");
                        }
                    });

                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                }catch (final Throwable e){

                    e.printStackTrace();
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            running = false;
                            status.setText("Import failed: " + e.getMessage());
                            Toast.makeText(MainActivity.this, "fail! " + e.toString(), Toast.LENGTH_LONG).show();
                        }
                    });
                } finally {
                    running = false;
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            bar.setProgress(100);
                        }
                    });
                }
            }
        }).start();
    }

    //TODO download the files from osm extracts
    //start the thing to process, be nice to have some guestimate of processing time
    //TODO make a ui for queries
}
