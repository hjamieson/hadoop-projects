package org.oclc.hbase.analytics.jmx.collector;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

/**
 * Interface defined the model of a class that will accept data written to it
 * from the JmxCollector.  This allows us to have different sinks, like stdout,
 * a file, or an ELK stack
 */
public interface JmxSink {
    /**
     * write an arbitrary string to the sink.
     * @param jsonString we expect this to be a real JSON string (no nl)
     * @throws IOException if write fails
     */
    void write(String jsonString) throws IOException;

    /**
     * writes a java.util.Map of string->objects to the sink.
     * @throws IOException
     */
    void write(Map<String, Object> map) throws IOException;

    /**
     * formally closes this sink.
     */
    void close();

    /**
     * lifecycle method that will be called before the sink is successfully opened.
     * This give the sink author the chance to do any setup needed before the sink
     * can be written to.
     * @param props optional bag of properties that can be passed to this method.
     */
    void init(Properties props) throws IOException;
}
