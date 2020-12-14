package org.oclc.hbase.analytics.jmx;

import org.oclc.hbase.tools.extractor.JmxCollector;
import org.oclc.hbase.tools.extractor.sink.StdoutSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * collections JMX data from rs and writes it to a local file.
 */
public class Jmx2File {
    private static final Logger LOG = LoggerFactory.getLogger(Jmx2File.class);

    public static void main(String[] args) {
        JmxCollector collector = new JmxCollector()
                .writeTo(new StdoutSink())
                .start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("in shutdown hook");
            collector.shutdown();
        }));

        while (true) {
            try {
                TimeUnit.SECONDS.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
