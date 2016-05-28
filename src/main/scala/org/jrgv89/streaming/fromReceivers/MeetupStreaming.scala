package org.jrgv89.streaming.fromReceivers

import org.apache.spark._
import org.apache.spark.streaming._
import org.jrgv89.streaming.Constants
import org.jrgv89.streaming.fromReceivers.receivers.HttpSocketReceiver
import org.jrgv89.utils.JSON

/**
  *
  */
object MeetupStreaming extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("ReceiverStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(DEFAULT_SECONDS))

  /* program code */

  val rdd = ssc.receiverStream(new HttpSocketReceiver("http://stream.meetup.com/2/rsvps"))
    .foreachRDD(
      x => {
        x.map(JSON.parseJSON)
          .foreach(
            y => {
              println(y)
            }
          )
      }
    )

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}

