package org.jrgv89.batch

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 21/5/16.
  */
object SimpleTextProcess extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("SimpleTextProcess")
  val sc: SparkContext = new SparkContext(sparkConf)

  /* program code */

  val lines = sc.textFile("assets/text.txt")

  //    Número de lineas
  //    Line numbers

  println(lines.count())


  //    Número de palabras de cada linea
  //    Number of words per line

  lines
    .zipWithIndex()
    .map(
      x => {
        (x._2 + 1, x._1.split("\\W+").length)
      }
    )
    .collect()
    .sortBy(_._1)
    .foreach(println)


  //    Número total de palabras.
  //    Total number of words.

  println(
    lines
      .map(
        x => {
          x.split("\\W+").length
        }
      )
      .sum()
  )

  //    Número de letras por linea
  //    Letter numbers per line

  lines
    .zipWithIndex()
    .map(
      x => {
        (x._2 + 1,
          x._1.split("\\W+")
            .flatMap(
              x => {
                x
              }
            )
            .length)
      }
    )
    .collect()
    .sortBy(_._1)
    .foreach(println)


  //    Número total de letras.
  //    Total number of letters.

  println(
    lines
      .map(
        x => {
          x.split("\\W+")
            .flatMap(
              x => {
                x
              }
            )
            .length
        }
      )
      .sum()
  )

  //    Palabra que aparece más veces
  //    Word that appears more times

  lines
    .flatMap(
      x => {
        x.split("\\W+")
      }
    )
    .map(
      x => {
        (x.toLowerCase, 1)
      }
    )
    .reduceByKey(_ + _)
    .sortBy(-_._2)
    .foreach(println)
}
