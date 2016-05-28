package org.jrgv89.streaming.fromKafka

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Important: Before start, launch kafka and create topic test
  * prints the messages from kafka from the offset saved in cassandra
  * by topic and partition and save the new offset iin cassandra too
  */
object KafkaCassandraOffsetsStreaming extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("KafkaCassandraOffsetStreaming")
    //cassandra host
    .set("spark.cassandra.connection.host", DEFAULT_CASSANDRA_HOST)
  val ssc = new StreamingContext(sparkConf, Seconds(DEFAULT_SECONDS))
  val sc: SparkContext = ssc.sparkContext

  /* cassandra configuration */

  CassandraConnector(sparkConf).withSessionDo { session =>
    //    session.execute(s"DROP KEYSPACE IF EXISTS LearningProject")
    session.execute(s"CREATE KEYSPACE IF NOT EXISTS LearningProject WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(s"CREATE TABLE IF NOT EXISTS LearningProject.kafkaSimple  (topic TEXT PRIMARY KEY, value BIGINT)")
    session.execute(s"CREATE TABLE IF NOT EXISTS LearningProject.kafkaCounter (topic TEXT PRIMARY KEY, value COUNTER)")
    //    session.execute(s"TRUNCATE LearningProject.kafkaSimple")
    //    session.execute(s"TRUNCATE LearningProject.kafkaCounter")
  }

  /* kafka configuration */

  val kafkaParams =
    Map[String, String](
      "metadata.broker.list" -> DEFAULT_KAFKA_BROKER,
      "auto.offset.reset" -> "smallest")

  //creating topics and offsets. Get offsets from redis.
  var simpleTopicsAndOffsets: Map[TopicAndPartition, Long] = Map()
  var counterTopicsAndOffsets: Map[TopicAndPartition, Long] = Map()

  //two ways to manages offset

  def handler = (m: MessageAndMetadata[String, String]) => m

  /* program code */

  //-------- 1 simple table --------

  val simpleRdd = sc
    .cassandraTable("learningproject", "kafkasimple")
    .where("topic = ?", "test")

  //check if there are offsets

  if (!simpleRdd.isEmpty()) {
    simpleRdd.foreach(x => {
      simpleTopicsAndOffsets += (TopicAndPartition(x.getString("topic"), 0) -> (x.getLong("value") + 1))
    })
  } else {
    simpleTopicsAndOffsets += (TopicAndPartition("test", 0) -> 0L)
  }

  val simpleLogs = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](ssc, kafkaParams, simpleTopicsAndOffsets, handler)
    .foreachRDD {
      rdd =>
        rdd.foreach {
          x => {
            //save the last offset into redis
            //simple table
            val collection = sc.parallelize(Seq(("test", x.offset)))
            collection.saveToCassandra("learningproject", "kafkasimple", SomeColumns("topic", "value"))
            println(x)
          }
        }
    }


  //-------- 2 counter table --------

  val counterRdd = sc.cassandraTable("learningproject", "kafkacounter").where("topic = ?", "test")

  //check if there are offsets

  if (!counterRdd.isEmpty()) {
    counterRdd.foreach(x => {
      counterTopicsAndOffsets += (TopicAndPartition(x.getString("topic"), 0) -> x.getLong("value"))
    })
  } else {
    counterTopicsAndOffsets += (TopicAndPartition("test", 0) -> 0L)
  }

  val counterLogs = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, MessageAndMetadata[String, String]](ssc, kafkaParams, counterTopicsAndOffsets, handler)
    .foreachRDD {
      rdd =>
        rdd.foreach {
          x => {
            val collection = sc.parallelize(Seq(("test", 1)))
            collection.saveToCassandra("learningproject", "kafkacounter", SomeColumns("topic", "value"))
            println(x)
          }
        }
    }

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}


