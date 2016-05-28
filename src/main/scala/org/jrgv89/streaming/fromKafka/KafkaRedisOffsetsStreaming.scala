package org.jrgv89.streaming.fromKafka

import com.redis.RedisClientPool
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jrgv89.streaming.Constants

/**
  * Important: Before start, launch kafka and create topic test
  * prints the messages from kafka from the offset saved in redis
  * by topic and partition and save the new offset in redis too
  */
object KafkaRedisOffsetsStreaming extends App with Constants {

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

  //creating topics and offsets. Get offsets from redis.
  var topicsAndOffsets: Map[TopicAndPartition, Long] = Map()

  /* redis configuration */

  val redis = new RedisClientPool(DEFAULT_REDIS_HOST, DEFAULT_REDIS_PORT)

  val redisPrefix = "topic_"

  redis.withClient {
    client => {
      //Add 1 because we have saved the last offset read
      val offset = client.get(redisPrefix + "test").getOrElse("0").toLong + 1
      println("offset = " + offset)
      topicsAndOffsets += (TopicAndPartition("test", 0) -> offset)
    }
  }

  //TopicAndPartition(topic, partition of this topics)

  def handler = (m: MessageAndMetadata[String, String]) => m

  /* program code */

  val rdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](ssc, kafkaParams, topicsAndOffsets, handler)
    .foreachRDD { rdd =>
      rdd.foreach {
        x => {
          //save the last offset into redis
          redis.withClient {
            client => {
              client.set(redisPrefix + "test", x.offset)
            }
          }
          println(x)
        }
      }
    }

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}