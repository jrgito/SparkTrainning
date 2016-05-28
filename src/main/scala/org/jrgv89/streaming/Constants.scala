package org.jrgv89.streaming

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.Logging

import scala.io.Source
/**
  * trait for constants
  * Created by JRGv89 on 25/5/16.
  */
trait Constants extends Logging {

  /** Set reasonable logging levels for streaming if the user has not configured log4j. */
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      // We first log something to initialize Spark's default logging, then we override the
      // logging level.
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }

  val DEFAULT_MASTER: String = "local[2]"
  val DEFAULT_SECONDS: Int = 5
  val DEFAULT_SOCKET_HOST: String = "localhost"
  val DEFAULT_SOCKET_PORT: Int = 9999
  val DEFAULT_CASSANDRA_HOST: String = "localhost"
  val DEFAULT_REDIS_HOST: String = "localhost"
  val DEFAULT_REDIS_PORT: Int = 6379
  val DEFAULT_KAFKA_BROKER: String = "localhost:9092"
  setStreamingLogLevels()
  /**
    * set properties for twitter auth
    */
  def configureTwitterCredentials() {
    Source
      .fromFile("keys/twitter.txt")
      .getLines()
      .map(
        x => {
          val key = x.split("=")
          if (key(1).trim.isEmpty) {
            throw new Exception("Error setting authentication - value for " + key(0) + " not set")
          }
          ("twitter4j.oauth." + key(0).replace("api", "consumer"), key(1).trim())
        }
      )
      .foreach(
        key => {
          System.setProperty(key._1, key._2)
          println("\tProperty " + key._1 + " set as [" + key._2 + "]")
        }
      )
    println()
  }
}
