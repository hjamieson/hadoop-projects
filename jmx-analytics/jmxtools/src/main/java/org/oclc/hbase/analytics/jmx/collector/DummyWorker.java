package org.oclc.hbase.analytics.jmx.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * main collector worker thread.  Given a user-defined cycle time, poll each RS
 * for the stats and write each reponse to the sink.  The worker will require:
 * - thread pool
 * - the cycle time
 * - the destination sink
 *
 */
public class DummyWorker implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(DummyWorker.class);
    private ExecutorService es;
    private long cycleTime = -1l;
    private JmxSink sink;   // note that the sinks should be threadsafe!

    public DummyWorker(ExecutorService es, long cycleTime, JmxSink sink) {
        this.es = es;
        this.cycleTime = cycleTime;
        this.sink = sink;
    }

    @Override
    public Void call() throws Exception {
        if (cycleTime < 0){
            LOG.error("cycletime has not been set");
            throw new IllegalArgumentException("cycletime has not been set");
        }
        LOG.info("cycletime: {}", cycleTime);

        while (!Thread.currentThread().isInterrupted()){
            LOG.info("worker is working");
            // create some dummy work:
            Arrays.stream(new String[]{"hello", "World","each","day"}).forEach(word -> {
                es.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        sink.write(word);
                        return null;
                    }
                });
            });
            TimeUnit.SECONDS.sleep(cycleTime);
        }
        return null;
    }
}
