package com.ligadata.ZkNodeChildListenerTest

import scala.actors.threadpool.{ Executors, ExecutorService }
import org.apache.log4j._
import scala.io._
import scala.util.control.Breaks._
import com.ligadata.ZooKeeper._
import org.rogach.scallop._
import com.ligadata.Utils.Utils

object ZkNodeChildListenerTest {
  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val zkconnectstring = opt[String]("zkconnectstring", required = false, descr = "Zookeeper connection string", default = Some("localhost"))
    val zkpath = opt[String]("zkpath", required = false, descr = "Zookeeper path", default = Some("/ligadata/ZkNodeChildListenerTest"))
    val threads = opt[Int]("threads", required = false, descr = "Number of threads to write data into ZK Node", default = Some(1))
    val childs = opt[Int]("childs", required = false, descr = "Number of child to write data into ZK Node", default = Some(1))
    val zkwaittime = opt[Int]("zkwaittime", required = false, descr = "Time (in milli seconds) to wait between each message to ZK Node in each thread", default = Some(0))
    val runtime = opt[Int]("runtime", required = false, descr = "Total program runtime", default = Some(30000))
    val zksessiontimeout = opt[Int]("zksessiontimeout", required = false, descr = "Time (in milli seconds) of ZK session", default = Some(30000))
    val zkconnectiontimeout = opt[Int]("zkconnectiontimeout", required = false, descr = "Time (in milli seconds) of ZK connection", default = Some(30000))
  }

  private val LOG = Logger.getLogger(getClass);

  case class ListenerInfo(idx: Int, obj: Object, waitTm: Int);

  private def ChildListener(eventType: String, eventPath: String, eventPathData: Array[Byte], childs: Array[(String, Array[Byte])], userContext: Any): Unit = {
    val inst = userContext.asInstanceOf[ListenerInfo]
    inst.obj.synchronized {
      LOG.warn("%s: ChildListener => SenderId: %d, eventType:%s, eventPath:%s, eventPathData:%s".format(Utils.GetCurDtTmStr, inst.idx, eventType, eventPath, new String(eventPathData)))
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

    var childs = conf.childs()
    if (childs < 0)
      childs = 1

    val zkwaittime = conf.zkwaittime()

    val runtime = conf.runtime()

    val zkSessionTimeoutMs = conf.zksessiontimeout()
    val zkConnectionTimeoutMs = conf.zkconnectiontimeout()

    val distributionExecutor = Executors.newFixedThreadPool(threads * childs + 10)
    val zkChildListeners = new Array[ZooKeeperListener](threads)

    for (i <- 0 until childs) {
      val childPath = zkpath + "/child" + i.toString
      CreateClient.CreateNodeIfNotExists(zkcConnectString, childPath)
    }

    for (i <- 0 until threads) {
      zkChildListeners(i) = new ZooKeeperListener
      try {
		zkChildListeners(i).CreatePathChildrenCacheListener(zkcConnectString, zkpath, false, ChildListener, zkSessionTimeoutMs, zkConnectionTimeoutMs, ListenerInfo(i + 1, new Object(), zkwaittime))
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
              val childMod = i % childs
              val childPath = zkpath + "/child" + childMod.toString
              zkChildListeners(i).zkc.setData().forPath(childPath, dataVal.toString.getBytes)
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
      if (zkChildListeners(i) != null)
        zkChildListeners(i).Shutdown
    }
  }
}

