package org.jrgv89.streaming.fromReceivers.receivers

import java.io.{BufferedReader, InputStreamReader}

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.{HttpEntity, HttpResponse}
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * http get receiver for spark
  * @param host {String} host of the http get socket
  */
class HttpSocketReceiver(host: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {
  def onStart() {
        new Thread("Socket Receiver") {
          override def run() {
            receive()
          }
        }.start()
  }

  def onStop() {}

  private def receive() {
    try {
      val httpclient: CloseableHttpClient = HttpClientBuilder.create.build
      val httpget: HttpGet = new HttpGet(host)
      val response: HttpResponse = httpclient.execute(httpget)
      val entity: HttpEntity = response.getEntity
      val rd: BufferedReader = new BufferedReader(new InputStreamReader(entity.getContent))
      var line: String = null
      //FIX WARNING
      while ((line = rd.readLine) != null) {
        {
          store(line)
          //System.out.println(line)
        }
      }
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting", e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}
// TEST
//object app extends App {
//  val conf = new SparkConf().setMaster("local").setAppName("SocketStreaming")
//  val ssc = new StreamingContext(conf, Seconds(10))
//  val lines = ssc.receiverStream[String](new HttpSocketReceiver("http://stream.meetup.com/2/rsvps"))
//  lines.foreachRDD(x => x.foreach(y => println(y)))
//  ssc.start()
//  ssc.awaitTermination()
//}