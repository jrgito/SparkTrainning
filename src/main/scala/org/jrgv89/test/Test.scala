package org.jrgv89.test

import scala.io.Source


/**
  * Created by JRGv89 on 24/5/16.
  */
object Test extends App {

  //  val output = new BufferedWriter(new FileWriter("logs/writer1.txt",true))  //clears file every time
  //  output.append("New Line!")
  //  output.close()
  //
  //  val writer = new BufferedWriter(new OutputStreamWriter(
  //    new FileOutputStream(new File("logs/writer2.txt"),true), "UTF-8"))
  //  writer.write("hola")

  //
  //  val pw = new PrintWriter(new File("logs/hello.txt" ))
  //  pw.write("\nHello, world")
  //  pw.write("Hello, world")
  //  pw.close
  //
  //  // FileWriter
  //  val file = new File("logs/test.txt")
  //  val bw = new BufferedWriter(new FileWriter(file))
  //  bw.write("\nhola")
  //  bw.write("hola")
  //  bw.close()
  val configs = new scala.collection.mutable.HashMap[String, String] ()
  Source.fromFile("keys/twitter.txt").getLines().map(x => {
    val key = x.split("=")
    if (key(1).trim.isEmpty) {
              throw new Exception("Error setting authentication - value for " + key(0) + " not set")
    }
    ( "twitter4j.oauth." + key(0).replace("api", "consumer"), key(1).trim())
  }).foreach(key => {
    configs += key
    System.setProperty(key._1, key._2.trim)
  })

//println(configs)
//  println(Seq(
//          "apiKey" -> "whwOklKAae8guN95cVtrA",
//          "apiSecret" -> "3Mlw082vWP2SX7FizOmGZZGFzHITuepNLt4rcaRpI",
//          "accessToken" -> "893181764-G1fgLg8LPg27IL65oSKmpeKANhFb2ebwbH9eVg4",
//          "accessTokenSecret" -> "lpseGZZKBXLgtLWjuUv94brxAEpYSES6NYcfCuIIsaYoo")
//  )

//  configs.foreach {
//    case (key, value) => {
//      if (value.trim.isEmpty) {
//        throw new Exception("Error setting authentication - value for " + key + " not set")
//      }
//      val fullKey = "twitter4j.oauth." + key.replace("api", "consumer")
//      System.setProperty(fullKey, value.trim)
//      println("\tProperty " + fullKey + " set as [" + value.trim + "]")
//    }
//  }

//  Configuring Twitter OAuth
//  Map(apiKey -> whwOklKAae8guN95cVtrA, accessToken -> 1893181764-G1fgLg8LPg27IL65oSKmpeKANhFb2ebwbH9eVg4, apiSecret -> 3Mlw082vWP2SX7FizOmGZZGFzHITuepNLt4rcaRpI, accessTokenSecret -> lpseGZZKBXLgtLWjuUv94brxAEpYSES6NYcfCuIIsaYoo)
//  Property twitter4j.oauth.consumerKey set as [whwOklKAae8guN95cVtrA]
//  Property twitter4j.oauth.accessToken set as [1893181764-G1fgLg8LPg27IL65oSKmpeKANhFb2ebwbH9eVg4]
//  Property twitter4j.oauth.consumerSecret set as [3Mlw082vWP2SX7FizOmGZZGFzHITuepNLt4rcaRpI]
//  Property twitter4j.oauth.accessTokenSecret set as [lpseGZZKBXLgtLWjuUv94brxAEpYSES6NYcfCuIIsaYoo]

}


