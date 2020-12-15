package org.oclc.hbase.analytics.jmx.jmx2file;

import org.oclc.hbase.analytics.jmx.collector.JmxCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * collections JMX data from rs and writes it to a local file.
 */
public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        JmxCollector collector = new JmxCollector()
                .writeTo(new FileSink(new File("build/tmp/dummy.txt")))
                .setCycleSeconds(10)
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
