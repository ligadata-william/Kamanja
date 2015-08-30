package com.ligadata.ZkNodeSimpleListenerTest

import scala.actors.threadpool.{ Executors, ExecutorService }
import org.apache.log4j._
import scala.io._
import scala.util.control.Breaks._
import com.ligadata.ZooKeeper._
import org.rogach.scallop._
import com.ligadata.Utils.Utils

object ZkNodeSimpleListenerTest {
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val zkconnectstring = opt[String]("zkconnectstring", required = false, descr = "Zookeeper connection string", default = Some("localhost"))
    val zkpath = opt[String]("zkpath", required = false, descr = "Zookeeper path", default = Some("/ligadata/zknodesimplelistenertest"))
    val threads = opt[Int]("threads", required = false, descr = "Number of threads to write data into ZK Node", default = Some(1))
    val zkwaittime = opt[Int]("zkwaittime", required = false, descr = "Time (in milli seconds) to wait between each message to ZK Node in each thread", default = Some(0))
    val runtime = opt[Int]("runtime", required = false, descr = "Total program runtime", default = Some(30000))
    val zksessiontimeout = opt[Int]("zksessiontimeout", required = false, descr = "Time (in milli seconds) of ZK session", default = Some(30000))
    val zkconnectiontimeout = opt[Int]("zkconnectiontimeout", required = false, descr = "Time (in milli seconds) of ZK connection", default = Some(30000))
  }

  private val LOG = Logger.getLogger(getClass);

  case class ListenerInfo(idx: Int, obj: Object, waitTm: Int);

  private def SimpleListener(data: Array[Byte], userContext: Any): Unit = {
    val inst = userContext.asInstanceOf[ListenerInfo]
    inst.obj.synchronized {
      val receivedStr = new String(data)
      LOG.warn("%s: SimpleListener => Context: %s, receivedStr:%s".format(Utils.GetCurDtTmStr, userContext.toString, receivedStr))
      if (inst.waitTm > 0)
        Thread.sleep(inst.waitTm)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    val zkcConnectString = conf.zkconnectstring().trim
    if (zkcConnectString.size == 0) {
      LOG.error("Need zkcConnectString")
      return
    }

    val zkpath = conf.zkpath().trim
    if (zkpath.size == 0) {
      LOG.error("Need zkpath")
      return
    }

    var threads = conf.threads()
    if (threads < 0)
      threads = 1

    val zkwaittime = conf.zkwaittime()

    val runtime = conf.runtime()

    val zkSessionTimeoutMs = conf.zksessiontimeout()
    val zkConnectionTimeoutMs = conf.zkconnectiontimeout()

    val distributionExecutor = Executors.newFixedThreadPool(threads + 10)
    val zkSimpleListeners = new Array[ZooKeeperListener](threads)

    CreateClient.CreateNodeIfNotExists(zkcConnectString, zkpath)

    for (i <- 0 until threads) {
      zkSimpleListeners(i) = new ZooKeeperListener
      try {
        zkSimpleListeners(i).CreateListener(zkcConnectString, zkpath, SimpleListener, zkSessionTimeoutMs, zkConnectionTimeoutMs, ListenerInfo(i + 1, new Object(), zkwaittime))
      } catch {
        case e: Exception => {
          LOG.error("Failed to start a zookeeper session: " + e.getMessage())
        }
      }
    }

    for (i <- 0 until threads) {
      // Start Simple thread now
      distributionExecutor.execute(new Runnable() {
        override def run() = {
          val curIdx = i
          var dataVal: Long = 0
          var canRun = true
          val startTime = System.currentTimeMillis
          while (distributionExecutor.isShutdown == false && canRun) {
            val curTime = System.currentTimeMillis
            canRun = (curTime - startTime) < runtime
            if (canRun && distributionExecutor.isShutdown == false) {
              dataVal = dataVal + 1
              zkSimpleListeners(i).zkc.setData().forPath(zkpath, dataVal.toString.getBytes)
            }
          }
          LOG.warn("Idx:%s pushed %d values in %dms".format(curIdx + 1, dataVal, runtime))
        }
      })
    }

    LOG.warn("Sleeping for 1 day or CTRL + C")
    Thread.sleep(86400000)
    distributionExecutor.shutdown
    LOG.warn("Shutting down")
    Thread.sleep(5000) // Waiting another 5 secs to go down all other threads
    for (i <- 0 until threads) {
      if (zkSimpleListeners(i) != null)
        zkSimpleListeners(i).Shutdown
    }
  }
}

