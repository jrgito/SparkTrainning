package org.jrgv89.streaming.fromKafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jrgv89.streaming.Constants

/**
  * Important: Before start, launch kafka and create topic test
  * prints the messages from kafka from the selected offset by topic and partition
  */
object KafkaOffsetsStreaming extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("KafkaOffsetsStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(DEFAULT_SECONDS))

  /* kafka configuration */

  val kafkaParams =
    Map[String, String](
      "metadata.broker.list" -> DEFAULT_KAFKA_BROKER,
      "auto.offset.reset" -> "smallest")

  //TopicAndPartition(topic, partition of this topics)

  val topicsAndOffsets = Map(TopicAndPartition("test", 0) -> 0L)

  /* handler function */

  def handler = (m: MessageAndMetadata[String, String]) => m

  /* program code */

  val rdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](ssc, kafkaParams, topicsAndOffsets, handler)
    .foreachRDD {
      rdd =>
        rdd.foreach {
          x => {
            println(x)
          }
        }
    }

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}