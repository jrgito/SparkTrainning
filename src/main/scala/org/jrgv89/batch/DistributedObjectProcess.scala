package org.jrgv89.batch

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 21/5/16.
  */
object DistributedObjectProcess extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("DistributedTextProcess")
  val sc: SparkContext = new SparkContext(sparkConf)

  /* program code */
  case class Number(uno:String, dos:String) extends Serializable
  val lines = sc.objectFile[(String, String)]("logs/socket/distributedObject")

  lines.foreach(
    x=>{
      println(x._1, x._2)
    }
  )
  //    Número de lineas
  //    Line numbers

  println(lines.count())


}
