package com.ligadata.automation.unittests.api.setup

import java.io.{FileNotFoundException, File, IOException}
import java.net.InetSocketAddress

import org.apache.zookeeper.server.{ZooKeeperServer, ServerCnxnFactory}

/**
 * Created by wtarver on 4/23/15.
 */

object EmbeddedZookeeper {
  private var zk: EmbeddedZookeeper = null

  def instance: EmbeddedZookeeper = {
    if (zk == null) {
      zk = new EmbeddedZookeeper(-1, 500)
    }
    return zk
  }


  protected class EmbeddedZookeeper(private var port: Int, private var tickTime: Int) {
    private val logger = org.apache.log4j.Logger.getLogger(this.getClass)
    private var factory: ServerCnxnFactory = _
    private var snapshotDir: File = _
    private var logDir: File = _
    private var isRunning: Boolean = false

    port = resolvePort(port)

    def this(port: Int) {
      this(port, 500)
    }

    def this() {
      this(-1, 500)
    }

    private def resolvePort(port: Int): Int = {
      if (port == -1) {
        val availablePort = TestUtils.getAvailablePort
        logger.debug("AUTOMATION-EMBEDDED-ZOOKEEPER: Resolving port '" + port + "' to port '" + availablePort)
        return availablePort
      }
      return port
    }

    @throws(classOf[IOException])
    def startup: Unit = {
      if(!isRunning) {
        logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Starting Zookeeper...")
        this.factory = ServerCnxnFactory.createFactory(new InetSocketAddress("localhost", port), 1024)
        this.snapshotDir = TestUtils.constructTempDir("embedded-zk/snapshot")
        this.logDir = TestUtils.constructTempDir("embedded-zk/log")
        try {
          factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime))
          logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Zookeeper started")
          isRunning = true
        }
        catch {
          case e: InterruptedException => throw new EmbeddedZookeeperException("AUTOMATION-EMBEDDED-ZOOKEEPER: Failed to start embedded zookeeper instance with exception:\n" + e)
        }
      }
    }

    def shutdown: Unit = {
      if(isRunning) {
        logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Shutting down zookeeper")
        factory.shutdown()
        logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Zookeeper shutdown")
        try {
          logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Deleting zookeeper files")
          TestUtils.deleteFile(snapshotDir)
          TestUtils.deleteFile(logDir)
          logger.info("AUTOMATION-EMBEDDED-ZOOKEEPER: Zookeeper files deleted")
          isRunning = false
        }
        catch {
          case e: FileNotFoundException =>
        }
      }
    }

    def getConnection: String = {
      "localhost:" + port
    }

    def getPort = this.port

    def setPort(port: Int): Unit = {
      this.port = port
    }

    def setTickTime(tickTime: Int): Unit = {
      this.tickTime = tickTime
    }

    override def toString: String = {
      val sb: StringBuilder = new StringBuilder("EmbeddedZookeeper{")
      sb.append("connection=").append(getConnection)
      sb.append('}')
      sb.toString
    }
  }

}
