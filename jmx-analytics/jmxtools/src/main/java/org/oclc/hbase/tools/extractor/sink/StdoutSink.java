package org.oclc.hbase.tools.extractor.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class StdoutSink implements JmxSink, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(StdoutSink.class);

    @Override
    public void write(String jsonString) throws IOException {
        System.out.println(jsonString);
    }

    @Override
    public void write(Map<String, Object> map) throws IOException {
        throw new UnsupportedOperationException("not impl");
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
