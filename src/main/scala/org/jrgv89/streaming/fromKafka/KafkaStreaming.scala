package org.jrgv89.streaming.fromKafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jrgv89.streaming.Constants

/**
  * Important: Before start, launch kafka and create topic test
  * prints the messages from kafka
  */
object KafkaStreaming extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("KafkaStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(DEFAULT_SECONDS))

  /* kafka configuration */

  val kafkaParams =
    Map[String, String](
      "metadata.broker.list" -> DEFAULT_KAFKA_BROKER,
      "auto.offset.reset" -> "smallest")

  val topicsSet = Set[String]("test")

  /* code program */

  val rdd = KafkaUtils
    .createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
    .foreachRDD {
      rdd => {
        rdd
          .foreach(println)
      }
    }

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}
