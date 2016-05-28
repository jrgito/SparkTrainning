package org.jrgv89.batch

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 21/5/16.
  */
object CSVProcess extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("CSVProcess")
  val sc: SparkContext = new SparkContext(sparkConf)

  /* program code */

  val lines = sc.textFile("assets/pictures.csv")

  //    Media de nominaciones de las películas ganadoras
  //    Average nominations of the winning films

  val total = lines.count()

  val nominationSum = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val s = x._1.split(",")
        try {
          s(2).toInt
        } catch {
          case e: NumberFormatException => 0
        }
      }
    )
    .sum()

  println(nominationSum / total)

  //    Media en Metacritic, ordenado por género
  //    Metacritic average, sorted by gender of winning films

  val metacriticSum = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val s = x._1.split(",")
        try {
          s(8).toInt
        } catch {
          case e: NumberFormatException => 0
        }
      }
    )
    .sum()
  println(metacriticSum / total)

  //    Duración media de las películas ganadoras por décadas ordeandar por décadas
  //    Duration average of movies by decade sorted by decade

  val averageByDecade = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val s = x._1.split(",")
        try {
          (Math.floor(s(1).toInt / 10).toInt, (0, s(4).toInt))
        } catch {
          case e: NumberFormatException => (0, (0, 0))
        }
      }
    )
    .reduceByKey(
      (acc, cur) => {
        (acc._1 + 1, acc._2 + cur._2)
      }
    )
    .map(
      x => {
        (x._1 * 10, x._2._2 / x._2._1)
      }
    )
    .collect()
    .sortBy(-_._1)
    .foreach(println)


  //    ¿Cuántas películas ganadoras incluyen al menos una de las palabras de su título en la sinopsis?
  //    How many winning films includes at least one of the words of its title in the synopsis?

  val atLeastOneWorld = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val s = x._1.split(",")
        (s(0), s(9))
      }
    )
    .filter(x => {
      //possible improve???
      val n = x._1.split("\\W+")
      var f = false
      n.foreach(n => {
        if (x._2.contains(n)) f = true
        else f = false
      })
      f
    })
    .count()
  println(atLeastOneWorld)


  //    ¿Cuántas películas ganadoras incluyen todas las palabras de su título en la sinopsis?
  //    How many winning films include all the words of its title in the synopsis?
  val allWords = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val s = x._1.split(",")
        (s(0), s(9))
      }
    )
    .filter(x => {
      //possible improve???
      val n = x._1.split("\\W+")
      var f = true
      n.foreach(n => {
        if (f) f = x._2.contains(n)
      })
      f
    })
    .count()
  println(allWords)


  //    ¿Cuál es la desviación estándar del rating de las películas ganadoras en el siglo XXI?
  //    What is the standard deviation of the rating for the winning films in the XXI century?

  //    example:
  //    sqrt (sum 1->n ((i - avg(x))^2 )/n)
  //     9, 3, 8, 8, 9, 8, 9, 18
  //     n = 8
  //     avg = (9 + 3 + 8 + 8 + 9 + 8 + 9 + 18) / 8 = 9
  //    sqrt ((9-9)^2 + (3-9)^2 + (8-9)^2 + (8-9)^2 + (9-9)^2 + (8-9)^2 + (9-9)^2 + (18)^2 ) / n)


  //    val scores = sc.parallelize(
  //      Seq(
  //        7.8, 8.1, 7.8, 8.0, 8.0, 7.6, 8.0, 8.1, 8.5, 7.9, 8.1, 8.9, 7.2, 8.2, 8.5, 8.4,
  //        7.2, 7.7, 7.4, 8.4, 8.8, 8.9, 8.3, 8.6, 7.8, 8.0, 8.0, 7.9, 7.5, 8.0, 7.9, 8.0,
  //        7.9, 6.8, 8.4, 7.6, 8.3, 8.1, 6.9, 8.2, 6.8, 7.7, 8.3, 7.8, 6.7, 7.3, 8.3, 7.6,
  //        7.9, 7.4, 8.2, 8.0, 7.3, 8.6, 7.6, 7.8, 8.2, 8.2, 8.0, 7.3, 6.8, 7.8, 8.2, 7.4,
  //        8.0, 7.8, 8.1, 7.2, 8.3, 7.4, 8.1, 7.3, 7.8, 7.8, 8.2, 8.1, 8.1, 8.7, 9.0, 8.3,
  //        9.2, 6.1, 7.6, 6.0, 8.1, 6.4, 7.8
  //      )
  //    )
  //    val count = scores.count
  //    val mean = scores.sum / count
  //    val devs = scores.map(score => (score - mean) * (score - mean))
  //    val stddev = Math.sqrt(devs.sum / count)
  //    println(stddev)


  val avg = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        x._1.split(",")(3).toDouble
      }
    ).sum() / total

  val sumOfPow = lines
    .zipWithIndex()
    .filter(_._2 > 0)
    .map(
      x => {
        val n = x._1.split(",")(3).toDouble
        (n - avg) * (n - avg)
      }
    ).sum()

  println(Math.sqrt(sumOfPow / total))
}
