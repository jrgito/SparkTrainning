package org.jrgv89.batch

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 22/5/16.
  */
object ProcessWithClasses extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("ProcessWithClasses")
  val sc: SparkContext = new SparkContext(sparkConf)

  /* program code */

  case class OlympicMedalRecords(name: String, age: Int, country: String, olympicGame: Int, sport: String, goldMedals: Int,
                                 silverMedals: Int, bronzeMedals: Int)

  val file = sc.textFile("assets/OlympicAthletes.csv")

  val rdd = file.map(x => {
    val arr = x.split(",")
    new OlympicMedalRecords(arr(0), Integer.parseInt(arr(1)), arr(2)
      , Integer.parseInt(arr(3)), arr(5), Integer.parseInt(arr(6)),
      Integer.parseInt(arr(7)), Integer.parseInt(arr(8)))
  })

  //  Haz un proceso que compare las medallas que han ganado China y EEUU a lo largo de la historia.
  //  Make a process that compare the medals that China and the US have won in the whole history.

  rdd
    .filter(
      x => {
        x.country.equalsIgnoreCase("china") || x.country.equalsIgnoreCase("United States")
      }
    )
    .map(
      x => {
        (x.country, (x.goldMedals, x.silverMedals, x.bronzeMedals))
      }
    )
    .aggregateByKey(0, 0, 0)(
      (a, n) => {
        (a._1 + n._1, a._2 + n._2, a._3 + n._3)
      },
      (l, r) => {
        (l._1 + r._1, l._2 + r._2, l._3 + r._3)
      }
    )
    //the same result
    //      .reduceByKey(
    //        (a, n) => {
    //          (a._1 + n._1, a._2 + n._2, a._3 + n._3)
    //        }
    //      )
    .foreach(println)

  //  Haz un proceso que compare las medallas que han ganado China y EEUU por cada año.
  //  Make a process that compare the medals that China and the US have won by each year.

  rdd
    .filter(
      x => {
        x.country.equalsIgnoreCase("china") || x.country.equalsIgnoreCase("United States")
      }
    )
    .map(
      x => {
        ((x.olympicGame, x.country), (x.goldMedals, x.silverMedals, x.bronzeMedals))
      }
    )
    .aggregateByKey(0, 0, 0)(
      (a, n) => {
        (a._1 + n._1, a._2 + n._2, a._3 + n._3)
      },
      (l, r) => {
        (l._1 + r._1, l._2 + r._2, l._3 + r._3)
      }
    )
    //      the same result
    //      .reduceByKey(
    //        (a, n) => {
    //          (a._1 + n._1, a._2 + n._2, a._3 + n._3)
    //        }
    //      )
    .collect()
    .sortBy(x => -x._1._1)
    .foreach(println)

  //  Dando puntos a cada una de las medallas (3 puntos oro, 2 puntos plata y 1 bronce) calcula el máximo de puntos que ha conseguido en todos los juegos olímpicos que ha participado.
  //  Giving points to each of the medals (3 points gold, 2 silver and 1 bronze points) calculates the maximum of points achieved in all Olympic Games has participated.

  rdd
    .map(
      x => {
        ((x.olympicGame, x.country), x.goldMedals * 3 + x.silverMedals * 2 + x.bronzeMedals * 1)
      }
    )
    .reduceByKey(_ + _)
    .map(
      x => {
        (x._1._2, x._2)
      }
    )
    .reduceByKey((a, n) => {
      if (a > n) {
        a
      } else {
        n
      }
    })

    //      the same result
    //      .aggregateByKey(0)(
    //        (a, n) => {
    //          a + n
    //        },
    //        _ + _
    //      )
    //      .map(
    //      x => {
    //        (x._1._2, x._2)
    //      }
    //    )
    //      .aggregateByKey(0)(
    //        (a, n) => {
    //          if (n > a)
    //            n
    //          else
    //            a
    //        },
    //        (l, r) => {
    //          if (r > l)
    //            r
    //          else
    //            l
    //        }
    //      )

    .collect()
    .sortBy(_._2)
    .foreach(println)

  //  Recupera a los tres mejores medallistas olímpicos (por numero de medallas) en cada una de los juegos olímpicos.
  //  Retrieves the top three Olympic medalists (by number of medals) in each of the Olympic Games.

  rdd
    .map(
      x => {
        (x.olympicGame, (x.name, x.goldMedals + x.silverMedals + x.bronzeMedals))
      }
    )
    .aggregateByKey(List[(String, Int)]())(
      (a, n) => {
        val newList = n :: a
        newList.sortBy(x => x._2).reverse.take(3)
      },
      (l, r) => {
        val newList = l ::: r
        newList.sortBy(x => x._2).reverse.take(3)
      }
    )
    //      .collect()
    .sortBy(_._1)
    .foreach(println)
}