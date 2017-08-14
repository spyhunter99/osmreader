package org.osmdroid.reader;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * created on 8/13/2017.
 *
 * @author Alex O'Ree
 */

public class PooledConvertor {

    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<Runnable>();
    ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors()*2, 5000, TimeUnit.MILLISECONDS, queue);

    public void enqueue(File input, File output) {
        pool.execute(new ConversionJob(input, output));
    }

    public void waitUntilFinished() {
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          }
    }

}
