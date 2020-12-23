package org.oclc.hbase.analytics.jmx.kafka;

import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaHelperTest {
    @Test
    void testLoadClasspath() {
        try {
            Properties p = KafkaHelper.loadKafkaProperties(Optional.empty());
            assertEquals("hddev1mb004dxc1.dev.oclc.org:9092", p.getProperty("bootstrap.servers"));
        } catch (IOException e) {
            fail(e);
        }
    }

    @Test
    void testLoadMissing() {
        assertThrows(FileNotFoundException.class, () -> {
            Properties p = KafkaHelper.loadKafkaProperties(Optional.of("missing.properties"));
        });
    }
}
