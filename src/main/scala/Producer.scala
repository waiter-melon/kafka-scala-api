/**
  * Kafka Producer
  */
object Producer extends App {

  import java.util.Properties

  import org.apache.kafka.clients.producer._

  val props = new Properties()

  /** go to the doc about how to configure a consumer */
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "1") // acknowledgement from the leader
  props.put("retries", "3") // acknowledgement from the leader

  val producer = new KafkaProducer[String, String](props)

  val TOPIC = "first_topic" // records with the same keys go to the same partition

  for (i <- 1 to 10) {
    val record = new ProducerRecord(TOPIC, Integer.toString(i), s"hello $i")
    producer.send(record, (metadata, exception) => println(metadata))
  }

  producer.close()
}