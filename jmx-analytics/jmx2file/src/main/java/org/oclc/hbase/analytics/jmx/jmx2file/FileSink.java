package org.oclc.hbase.analytics.jmx.jmx2file;

import org.oclc.hbase.analytics.jmx.collector.JmxSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class FileSink implements JmxSink, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FileSink.class);
    private File file;
    private BufferedWriter writer;

    public FileSink(File file) {
        this.file = file;
    }

    @Override
    public void write(String jsonString) throws IOException {
        synchronized (this) {
            writer.append(jsonString).append("\n");
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
        try {
            writer.flush();
        } catch (IOException e) {
            LOG.error("flush failure");
        }
    }

    @Override
    public void init(Properties props) throws IOException {
        LOG.info("initializing {}", this.getClass().getSimpleName());
        writer = new BufferedWriter(new FileWriter(this.file));
    }
}
