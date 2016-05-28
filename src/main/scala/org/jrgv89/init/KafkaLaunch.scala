package org.jrgv89.init

import java.util.concurrent._
import java.util.{Date, Properties}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.server.{KafkaConfig, KafkaServerStartable}
import kafka.utils.Logging


object KafkaLaunch extends App {

  override def main(args: Array[String]) {
    val props: Properties = new Properties
    props.setProperty("hostname", "localhost")
    props.setProperty("port", "9092")
    props.setProperty("broker.id", "0")
    props.setProperty("group.id", "0")
    props.setProperty("log.dir", "logs/kafka/" + new Date().getTime)
    props.setProperty("zookeeper.connect", "localhost:2181")
    props.setProperty("zookeeper.connection.timeout.ms", "6000")
    props.setProperty("log.flush.interval", "1")
    props.setProperty("log.default.flush.scheduler.interval.ms", "1")
    props.setProperty("num.partitions", "1")
    val kafkaConfig: KafkaConfig = new KafkaConfig(props)
    new KafkaServerStartable(kafkaConfig).startup
    System.out.println("done")
    val example = new ScalaConsumer("localhost:2181", "group1", "test", 0L)
    example.run(10)
  }


  class ScalaConsumer(val zookeeper: String, val groupId: String, val topic: String, val delay: Long) extends Logging {

    val config = createConsumerConfig(zookeeper, groupId)
    val consumer = Consumer.create(config)
    var executor: ExecutorService = null

    def shutdown() = {
      if (consumer != null)
        consumer.shutdown()
      if (executor != null)
        executor.shutdown()
    }

    def createConsumerConfig(zookeeper: String, groupId: String): ConsumerConfig = {
      val props = new Properties()
      props.put("zookeeper.connect", zookeeper)
      props.put("group.id", groupId)
      props.put("auto.offset.reset", "smallest")
      props.put("zookeeper.session.timeout.ms", "400")
      props.put("zookeeper.sync.time.ms", "200")
      props.put("auto.commit.interval.ms", "1000")
      val config = new ConsumerConfig(props)
      config
    }

    def run(numThreads: Int) = {
      val topicCountMap = Map(topic -> numThreads)
      val consumerMap = consumer.createMessageStreams(topicCountMap)
      val streams = consumerMap.get(topic).get
      executor = Executors.newFixedThreadPool(numThreads)
      var threadNumber = 0
      for (stream <- streams) {
        executor.submit(new ScalaConsumerTest(stream, threadNumber, delay))
        threadNumber += 1
      }
    }
  }


  class ScalaConsumerTest(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadNumber: Int, val delay: Long) extends Logging with Runnable {
    def run {
      val it = stream.iterator()

      while (it.hasNext()) {
        val msg = new String(it.next().message())
        System.out.println(System.currentTimeMillis() + ",Thread " + threadNumber + ": " + msg)
      }

      System.out.println("Shutting down Thread: " + threadNumber)
    }
  }
}