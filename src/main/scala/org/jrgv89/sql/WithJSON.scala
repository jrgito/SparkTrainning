package org.jrgv89.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.jrgv89.streaming.Constants

/**
  * Created by JRGv89 on 3/6/16.
  */
object WithJSON extends App with Constants {

  /* spark configuration */

  val sparkConf = new SparkConf()
    .setMaster(DEFAULT_MASTER)
    .setAppName("JsonSQL")
  val sc: SparkContext = new SparkContext(sparkConf)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  /* program code */

  // A JSON dataset is pointed to by path.
  // The path can be either a single text file or a directory storing text files.
  val people = sqlContext.read.json("assets/people.json")

  // The inferred schema can be visualized using the printSchema() method.
  people.printSchema()

  people.show()

  people.select("name").show()

  println(people.count())

  people.filter(people("age") > 40 ).show()

  people.filter("age > 40 and gender = 'M'").show()

}
