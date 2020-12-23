package org.oclc.hbase.analytics.jmx.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Optional;
import java.util.Properties;

/**
 * Utils that make working with Kafka a little more simple.
 */
public class KafkaHelper {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaHelper.class);
    public static final String KAFKA_PROPERTIES = "/kafka.properties";

    /**
     * search a few well-known locations for kafka properties, including a suggested one!
     * search order:
     * 1: path goven
     * 2: classpath (kafka.properties)
     * 3: current working dir (kafka.properties)
     *
     * @param path
     * @return
     */
    public static Properties loadKafkaProperties(Optional<String> path) throws IOException {
        Properties p = new Properties();
        String actualFile = path.isPresent()? path.get() : KAFKA_PROPERTIES;
        // if path is given, use that over all
            try (Reader reader = new FileReader(new File(actualFile))) {
                p.load(reader);
                return p;
            } catch (Exception e) {
                LOG.warn("{} not found in files", actualFile);
            }
        // lets check the classpath:
        try (Reader reader = new InputStreamReader(KafkaHelper.class.getResourceAsStream(actualFile))) {
            p.load(reader);
            return p;
        } catch (Exception e) {
            LOG.warn("{} not found on classpath", actualFile);
        }

        throw new FileNotFoundException("Unable to load kafka properties file!");

    }
}
