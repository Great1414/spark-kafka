
package com.sparkstreaming
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

object consumer {
  def main(args: Array[String]): Unit = {
    var conf = new SparkConf().setMaster("spark://xxx")
      .setAppName("demo")
    var ssc = new StreamingContext(conf, Seconds(4))

    var topic = Array("test")
    var group = "consumer"
    var kafkaParam = Map(
      "bootstrap.servers" -> "192.168.120.130:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    var stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent, Subscribe[String, String](topic, kafkaParam))
    stream.map(s => (s.key(), s.value())).print()
    ssc.start()
    ssc.awaitTermination()













  }
}
