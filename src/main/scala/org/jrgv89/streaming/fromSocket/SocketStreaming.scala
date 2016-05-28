package org.jrgv89.streaming.fromSocket

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jrgv89.streaming.Constants


/**
  * Simple socket streaming app. Prints messages from socket
  * Important: execute in a terminal cmd nc -lk 9999
  */

object SocketStreaming extends App with Constants {
  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("SocketStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(1))

  /* program code */

  // print the message from the socket
  val lines = ssc
        .socketTextStream(DEFAULT_SOCKET_HOST, DEFAULT_SOCKET_PORT)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
//  val wordCounts = pairs.reduceByKey(_ + _)
  val wordCounts = pairs.reduceByKeyAndWindow((a:Int,b:Int) => (a + b), Seconds(5), Seconds(2))

  // Print the first ten elements of each RDD generated in this DStream to the console
  wordCounts.print()


//    val rdd = ssc
//      .socketTextStream(DEFAULT_SOCKET_HOST, DEFAULT_SOCKET_PORT)
//    words  .foreachRDD(
//        rdd => {
//          rdd.foreach(println)
//        }
//      )

  // save the massage as object file
//case class Number(uno:String, dos:String) extends Serializable
//  val rdd = ssc
//    .socketTextStream("localhost", 9999)
//    .foreachRDD(
//      rdd => {
//        rdd
//          .map(
//            x => {
//              val m = x.split(",")
//              if (m.length > 1) {
//                (m(0), m(1))
//              } else {
//                (m(0), null)
//              }
//            }
//          )
//          .saveAsObjectFile("logs/socket/distributedObject")
//      }
//    )

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}

