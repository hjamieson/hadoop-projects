package org.oclc.hbase.analytics.jmx.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.oclc.hbase.analytics.jmx.collector.JmxSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

/**
 * A sink fit to send jmx data to Kafka.  Naturally, we'll need a topic and a few
 * other little goodies to make it happen.
 *
 * if you tinker here, make sure you do things in a threadsafe fashion!
 */
public class KafkaSink implements JmxSink, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink.class);
    private Optional<String> optionalKafkaPropertiesPath = Optional.empty();
    private KafkaProducer<String, String> producer;
    private String topic;

    public KafkaSink(String topic) {
        this.topic = topic;
    }

    public KafkaSink(String topic, Optional<String> optionalKafkaPropertiesPath) {
        this.optionalKafkaPropertiesPath = optionalKafkaPropertiesPath;
        this.topic = topic;
    }

    public KafkaSink(Optional<String> optionalKafkaPropertiesPath, String topic, Callback callback) {
        this.optionalKafkaPropertiesPath = optionalKafkaPropertiesPath;
        this.topic = topic;
        this.callback = callback;
    }

    /**
     * write the json to kafka.  We don't synchronize here because the producer is
     * already threadsafe!
     * @param jsonString we expect this to be a real JSON string (no nl)
     * @throws IOException
     */
    @Override
    public void write(String jsonString) throws IOException {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, nextKey(), jsonString);
        producer.send(record, callback);
    }

    @Override
    public void write(Map<String, Object> map) throws IOException {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void close() {
        producer.flush();
        producer.close();
        LOG.debug("producer closed");
    }

    @Override
    public void init(Properties props) throws IOException {
        producer = new KafkaProducer<String, String>(KafkaHelper.loadKafkaProperties(optionalKafkaPropertiesPath));
    }

    /**
     * this generates the next key to be used to send a message.
     * todo its just a constant for the time being.
     *
     * @return the next key to be used.
     */
    private String nextKey() {
        return "rsbean";
    }

    /*
    the default callback for the producer.
     */
    private Callback callback = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                LOG.error("callback error:", exception);
            } else {
                LOG.debug("callback: send complete");
            }
        }
    };
}
