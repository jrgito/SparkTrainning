package org.jrgv89.batch

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 21/5/16.
  */
object DistributedTextProcess extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("DistributedTextProcess")
  val sc: SparkContext = new SparkContext(sparkConf)

  /* program code */

  val lines = sc.textFile("logs/twitter/distributedFile.txt")

  lines.foreach(println)
  //    Número de lineas
  //    Line numbers

  println(lines.count())


}
