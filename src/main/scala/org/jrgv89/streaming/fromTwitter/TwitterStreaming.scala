package org.jrgv89.streaming.fromTwitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.jrgv89.streaming.Constants

/**
  * Creates a rdd from twitter and print the processed messages
  * Created by JRGv89 on 23/5/16.
  */
object TwitterStreaming extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("TwitterStreaming")
  val ssc = new StreamingContext(sparkConf, Seconds(DEFAULT_SECONDS))

  /* configure twitter */

  configureTwitterCredentials()

  /* program code */

  val rdd = TwitterUtils.createStream(ssc, None)
    .foreachRDD(
      (rdd) => {
        rdd
          .filter(
            x => {
              x.getText.contains("#") && x.getLang.equals("es")
            }
          )
          .map(
            x => {
              (x.getQuotedStatus, x.getLang, x.getPlace, x.getUser.getLang, x.getUser.getLocation, x.getText)
            }
          )
          .collect()
          .foreach(
            user => {
              println(user)
            }
          )
      }
    )

  /* start and wait for new rdd */

  ssc.start()
  ssc.awaitTermination()
}
