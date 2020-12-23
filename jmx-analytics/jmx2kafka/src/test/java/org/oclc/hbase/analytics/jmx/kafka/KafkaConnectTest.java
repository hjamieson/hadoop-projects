package org.oclc.hbase.analytics.jmx.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class KafkaConnectTest {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectTest.class);
    @Test
    void testProduce() {
        // send a few messages to Kafka topic="org.hugh.test"
        String topic = "dbahadoop.hugh.test";
        AtomicInteger counter = new AtomicInteger(0);
        Properties props = getKafkaProperties();
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            Arrays.asList(new String[]{"java", "is", "fun"})
                    .stream()
                    .forEach(word -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, word, word);
                        producer.send(record, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                System.out.println("send completed : " + counter.incrementAndGet());
                            }
                        });
                    });
            // lets wait a bit;
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
            }
            assertEquals(3, counter.get());
        }
    }

    @Test
    void testProduceWithHelper(){
        String topic = "dbahadoop.hugh.test";
        AtomicInteger counter = new AtomicInteger(0);
        Properties props = null;
        try {
            props = KafkaHelper.loadKafkaProperties(Optional.of("/test.kafka.properties"));
        } catch (IOException e) {
            fail(e);
        }
        try (KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props)) {
            Arrays.asList(new String[]{"java", "is", "fun"})
                    .stream()
                    .forEach(word -> {
                        ProducerRecord<String, String> record = new ProducerRecord<>(topic, word, word);
                        producer.send(record, new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                System.out.println("send completed : " + counter.incrementAndGet());
                            }
                        });
                    });
            // lets wait a bit;
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
            }
            assertEquals(3, counter.get());
        }

    }

    private Properties getKafkaProperties() {
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hddev1mb004dxc1.dev.oclc.org:9092");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.ACKS_CONFIG, "1");
        return p;
    }
}
