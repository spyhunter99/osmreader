package org.osmdroid.reader.downloads;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class Downloader {

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 4, 5000, TimeUnit.MILLISECONDS, queue);

    public void enqueue(String url, File dest) {
        pool.execute(new DownloadJob(url, dest));
    }

    public void waitUntilFinished() {
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }
    }

    private static class DownloadJob implements Runnable {
        File dest;
        String url;

        public DownloadJob(String url, File dest) {
            this.url = url;
            this.dest = dest;
        }

        @Override
        public void run() {
            ReadableByteChannel rbc = null;
            try {
                rbc = Channels.newChannel(new URL(url).openStream());

                FileOutputStream fos = new FileOutputStream(dest);
                fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
                fos.close();
                rbc.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
