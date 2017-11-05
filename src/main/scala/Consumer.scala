import java.util

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}

import scala.collection.JavaConverters._

object Consumer extends App {

  import java.util.Properties

  val TOPIC = "first_topic"

  val props = new Properties()

  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

  /**
    * If I launch multiple instances of this app I will obtain multiple consumers sharing
    * the same groupd id. This means that the partitions related to the topic will be
    * assigned across the consumers and each consumer will read exclusively from assigned
    * partitions.
    */
  props.put("group.id", "something")
  props.put("enable.auto.commit", "true") // commits the offset periodically
  props.put("auto.commit.interval.ms", "1000") // offset committed every second
  props.put("auto.offset.reset", "latest")

  val consumer = new KafkaConsumer[String, String](props)

  consumer.subscribe(util.Collections.singletonList(TOPIC))

  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(100) // poll for 100 seconds

    for (record <- records.asScala) {
      println("val: " + record.value + " key: " + record.key + " offset: " + record.offset + " partition: " + record.partition)
    }
  }

}