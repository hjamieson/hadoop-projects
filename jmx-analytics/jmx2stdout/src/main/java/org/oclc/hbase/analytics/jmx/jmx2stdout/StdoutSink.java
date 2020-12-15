package org.oclc.hbase.analytics.jmx.jmx2stdout;

import org.oclc.hbase.analytics.jmx.collector.JmxSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class StdoutSink implements JmxSink, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdoutSink.class);

    @Override
    public void write(String jsonString) throws IOException {
        synchronized (this) {
            System.out.println(jsonString);
            System.out.flush();
        }
    }

    @Override
    public void write(Map<String, Object> map) throws IOException {
        synchronized (this) {
            throw new UnsupportedOperationException("not impl");
        }
    }

    @Override
    public void close() {
        LOG.info("closing");
        System.out.flush();
    }

    @Override
    public void init(Properties props) {
        LOG.info("initializing {}", this.getClass().getSimpleName());
    }
}
