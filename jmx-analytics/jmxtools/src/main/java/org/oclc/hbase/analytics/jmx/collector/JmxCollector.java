package org.oclc.hbase.analytics.jmx.collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class JmxCollector implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(JmxCollector.class);

    public static final int THREAD_POOL_SIZE = 20;
    private int cycleSecs = 5;
    private ExecutorService es;
    private JmxSink sink;

    /**
     * starts execution of the collector.
     * @return this
     */
    public JmxCollector start(){
        LOG.info("starting");
        if (sink == null){
            throw new IllegalStateException("no sink configured; aborting..");
        }
        sink.init(new Properties());

        es = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        es.submit(new Worker(es, cycleSecs, sink));
        return this;
    }

    /**
     * shutdown this collector.
     */
    public void shutdown(){
        LOG.info("shutting down");
        es.shutdownNow();
        sink.close();
    }

    /**
     * connects the connector to the destination for writes.
     * @param sink
     * @return
     */
    public JmxCollector writeTo(JmxSink sink){
        this.sink = sink;
        return this;
    }

    @Override
    public void run() {

    }

    public int getCycleSecs() {
        return cycleSecs;
    }

    public JmxCollector setCycleSeconds(int seconds){
        this.cycleSecs = seconds;
        return this;
    }
}
