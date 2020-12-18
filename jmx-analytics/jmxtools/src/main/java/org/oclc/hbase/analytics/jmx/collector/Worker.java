package org.oclc.hbase.analytics.jmx.collector;

import org.oclc.hbase.analytics.jmx.HbaseUtils;
import org.oclc.hbase.analytics.jmx.JmxUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
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
public class Worker implements Callable<Void> {

    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    private ExecutorService es;
    private long cycleTime = -1l;
    private JmxSink sink;   // note that the sinks should be threadsafe!

    public Worker(ExecutorService es, long cycleTime, JmxSink sink) {
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
        LOG.debug("cycletime: {}", cycleTime);

        try {
            while (!Thread.currentThread().isInterrupted()) {
                LOG.debug("worker is working");
                HbaseUtils.getRegionServersList().stream().forEach(rs -> {
                    es.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            LOG.debug("polling {}", rs);
                            sink.write(JmxUtil.getJmxAsJson(rs, 60030));
                            return null;
                        }
                    });
                });
                TimeUnit.SECONDS.sleep(cycleTime);
            }
        }catch (InterruptedException ie){
            LOG.debug("worker done");
        } catch (Throwable e) {
            LOG.error(e.getMessage(), e);
        }
        return null;
    }
}
