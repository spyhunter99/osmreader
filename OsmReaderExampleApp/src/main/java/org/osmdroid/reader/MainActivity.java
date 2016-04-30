package org.osmdroid.reader;

import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.Toast;

import org.osmdroid.reader.example.R;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        new Thread(new Runnable() {
            @Override
            public void run() {

                try {
                    DriverManager.registerDriver((Driver) (Class.forName(
                             "org.sqldroid.SQLDroidDriver" , true,
                            getClass().getClassLoader()).newInstance()));
                    Connection con = DriverManager.getConnection("jdbc:sqldroid:/sdcard/osmdata3.sqlite");

                    final long now = System.currentTimeMillis();

                    new Reader().read("/sdcard/delaware-latest.osm.bz2", con);

                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            Toast.makeText(MainActivity.this, "success "+(System.currentTimeMillis()-now), Toast.LENGTH_LONG).show();
                        }
                    });
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                    System.out.println("successful import "+(System.currentTimeMillis()-now));
                }catch (final Exception e){
                    e.printStackTrace();
                    runOnUiThread(new Runnable() {
                        @Override
                        public void run() {
                            Toast.makeText(MainActivity.this, "fail! " + e.toString(), Toast.LENGTH_LONG).show();
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
