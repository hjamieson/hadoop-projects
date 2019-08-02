package example

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.FlatSpec

class PimpPublish extends FlatSpec {
  "A publisher" should "push a message to the broker" in {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hdckf1mb001pxh1.csb.oclc.org:9092")
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String,String](props)
    for (message <- List("able","baker","charlie")) {
      val md = producer.send(new ProducerRecord[String,String]("test-topic",message)).get()
      println(s"offset=${md.offset}")
      assert(md.offset() != 0)
    }
    producer.close()
  }

}
