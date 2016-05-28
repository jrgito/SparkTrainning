package org.jrgv89.init

/**
  * Created by JRGv89 on 23/5/16.
  */

import java.io.IOException
import java.util.{Date, Properties}

import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

object ZookeeperLaunch {
  def main(args: Array[String]) {
    val properties: Properties = new Properties
    properties.setProperty("dataDir", "logs/zookeeper/" + new Date().getTime + ".txt")
    properties.setProperty("clientPort", "2181")
    try {
      new ZooKeeperLocal(properties)
    }
    catch {
      case e: IOException => e.printStackTrace()

    }
    System.out.println("done")
  }

  class ZooKeeperLocal(zkProperties: Properties) {
    val quorumConfiguration: QuorumPeerConfig = new QuorumPeerConfig()
    try {
      quorumConfiguration.parseProperties(zkProperties)
    } catch {
      case e: NumberFormatException => throw new RuntimeException(e);
    }
    val zooKeeperServer = new ZooKeeperServerMain()
    val configuration: ServerConfig = new ServerConfig()
    configuration.readFrom(quorumConfiguration)
    new Thread() {
      override def run(): Unit = {
        try {
          zooKeeperServer.runFromConfig(configuration)
        } catch {
          case e: IOException => {
            println("ZooKeeper Failed")
            e.printStackTrace()
            throw new RuntimeException(e)
          }
        }
      }
    }.start()
  }

}

