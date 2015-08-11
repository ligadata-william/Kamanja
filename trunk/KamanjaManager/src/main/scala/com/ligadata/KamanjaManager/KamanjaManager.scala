
package com.ligadata.KamanjaManager

import com.ligadata.KamanjaBase._

import scala.reflect.runtime.{ universe => ru }
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import collection.mutable.{ MultiMap, Set }
import java.io.{ PrintWriter, File, PrintStream, BufferedReader, InputStreamReader }
import scala.util.Random
import scala.Array.canBuildFrom
import java.net.URL
import java.net.URLClassLoader
import java.util.Properties
import java.sql.Connection
import scala.collection.mutable.TreeSet
import java.net.{ Socket, ServerSocket }
import java.util.concurrent.{ Executors, ScheduledExecutorService, TimeUnit }
import com.ligadata.Utils.Utils
import org.apache.log4j.Logger
import com.ligadata.HeartBeat.HeartBeatUtil

class KamanjaServer(var mgr: KamanjaManager, port: Int) extends Runnable {
  private val LOG = Logger.getLogger(getClass);
  private val serverSocket = new ServerSocket(port)

  def run() {
    try {
      while (true) {
        // This will block until a connection comes in.
        val socket = serverSocket.accept()
        (new Thread(new ConnHandler(socket, mgr))).start()
      }
    } catch {
      case e: Exception => { LOG.error("Socket Error. Reason:%s Message:%s".format(e.getCause, e.getMessage)) }
    } finally {
      if (serverSocket.isClosed() == false)
        serverSocket.close
    }
  }

  def shutdown() {
    if (serverSocket.isClosed() == false)
      serverSocket.close
  }
}

class ConnHandler(var socket: Socket, var mgr: KamanjaManager) extends Runnable {
  private val LOG = Logger.getLogger(getClass);
  private val out = new PrintStream(socket.getOutputStream)
  private val in = new BufferedReader(new InputStreamReader(socket.getInputStream))

  def run() {
    val vt = 0
    try {
      breakable {
        while (true) {
          val strLine = in.readLine()
          if (strLine == null)
            break
          mgr.execCmd(strLine)
        }
      }
    } catch {
      case e: Exception => { LOG.error("Reason:%s Message:%s".format(e.getCause, e.getMessage)) }
    } finally {
      socket.close;
    }
  }
}

object KamanjaConfiguration {
  var configFile: String = _
  var allConfigs: Properties = _
  var metadataStoreType: String = _
  var metadataSchemaName: String = _
  var metadataLocation: String = _
  var dataStoreType: String = _
  var dataSchemaName: String = _
  var dataLocation: String = _
  var adapterSpecificConfig: String = _
  var statusInfoStoreType: String = _
  var statusInfoSchemaName: String = _
  var statusInfoLocation: String = _
  var statusInfoAdapterSpecificConfig: String = _
  var jarPaths: collection.immutable.Set[String] = _
  var nodeId: Int = _
  var clusterId: String = _
  var nodePort: Int = _
  var zkConnectString: String = _
  var zkNodeBasePath: String = _
  var zkSessionTimeoutMs: Int = _
  var zkConnectionTimeoutMs: Int = _

  // Debugging info configs -- Begin
  var waitProcessingSteps = collection.immutable.Set[Int]()
  var waitProcessingTime = 0
  // Debugging info configs -- End

  var shutdown = false
  var participentsChangedCntr: Long = 0

  def GetValidJarFile(jarPaths: collection.immutable.Set[String], jarName: String): String = {
    if (jarPaths == null) return jarName // Returning base jarName if no jarpaths found
    jarPaths.foreach(jPath => {
      val fl = new File(jPath + "/" + jarName)
      if (fl.exists) {
        return fl.getPath
      }
    })
    return jarName // Returning base jarName if not found in jar paths
  }

  def Reset: Unit = {
    configFile = null
    allConfigs = null
    metadataStoreType = null
    metadataSchemaName = null
    metadataLocation = null
    dataStoreType = null
    dataSchemaName = null
    dataLocation = null
    adapterSpecificConfig = null
    statusInfoStoreType = null
    statusInfoSchemaName = null
    statusInfoLocation = null
    statusInfoAdapterSpecificConfig = null
    jarPaths = null
    nodeId = 0
    clusterId = null
    nodePort = 0
    zkConnectString = null
    zkNodeBasePath = null
    zkSessionTimeoutMs = 0
    zkConnectionTimeoutMs = 0

    // Debugging info configs -- Begin
    waitProcessingSteps = null
    waitProcessingTime = 0
    // Debugging info configs -- End

    shutdown = false
    participentsChangedCntr = 0
  }
}

class KamanjaClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class KamanjaLoaderInfo {
  // class loader
  val loader: KamanjaClassLoader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String];

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)

  // ru.runtimeMirror(modelsloader)
}

class KamanjaManager {
  private val LOG = Logger.getLogger(getClass);

  // metadata loader
  private val metadataLoader = new KamanjaLoaderInfo

  // KamanjaServer Object
  private var serviceObj: KamanjaServer = null

  private val inputAdapters = new ArrayBuffer[InputAdapter]
  private val outputAdapters = new ArrayBuffer[OutputAdapter]
  private val statusAdapters = new ArrayBuffer[OutputAdapter]
  private val validateInputAdapters = new ArrayBuffer[InputAdapter]
  private var heartBeat: HeartBeatUtil = null

  private type OptionMap = Map[Symbol, Any]

  private def PrintUsage(): Unit = {
    LOG.warn("Available commands:")
    LOG.warn("    Quit")
    LOG.warn("    Help")
    LOG.warn("    --config <configfilename>")
  }

  private def Shutdown(exitCode: Int): Int = {
    if (KamanjaMetadata.envCtxt != null)
      KamanjaMetadata.envCtxt.PersistRemainingStateEntriesOnLeader
    if (heartBeat != null)
      heartBeat.Shutdown
    heartBeat = null
    KamanjaLeader.Shutdown
    KamanjaMetadata.Shutdown
    ShutdownAdapters
    if (KamanjaMetadata.envCtxt != null)
      KamanjaMetadata.envCtxt.Shutdown
    if (serviceObj != null)
      serviceObj.shutdown
    return exitCode
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        LOG.error("Unknown option " + option)
        throw new Exception("Unknown option " + option)
      }
    }
  }

  private def LoadDynamicJarsIfRequired(loadConfigs: Properties): Boolean = {
    val dynamicjars: String = loadConfigs.getProperty("dynamicjars".toLowerCase, "").trim

    if (dynamicjars != null && dynamicjars.length() > 0) {
      val jars = dynamicjars.split(",").map(_.trim).filter(_.length() > 0)
      if (jars.length > 0) {
        val qualJars = jars.map(j => KamanjaConfiguration.GetValidJarFile(KamanjaConfiguration.jarPaths, j))
        val nonExistsJars = KamanjaMdCfg.CheckForNonExistanceJars(qualJars.toSet)
        if (nonExistsJars.size > 0) {
          LOG.error("Not found jars in given Dynamic Jars List : {" + nonExistsJars.mkString(", ") + "}")
          return false
        }
        return ManagerUtils.LoadJars(qualJars.toArray, metadataLoader.loadedJars, metadataLoader.loader)

      }
    }

    true
  }

  private def ShutdownAdapters: Boolean = {
    LOG.debug("Shutdown Adapters started @ " + Utils.GetCurDtTmStr)
    val s0 = System.nanoTime

    validateInputAdapters.foreach(ia => {
      ia.Shutdown
    })

    validateInputAdapters.clear

    inputAdapters.foreach(ia => {
      ia.Shutdown
    })

    inputAdapters.clear

    outputAdapters.foreach(oa => {
      oa.Shutdown
    })

    outputAdapters.clear

    statusAdapters.foreach(oa => {
      oa.Shutdown
    })

    statusAdapters.clear

    val totaltm = "TimeConsumed:%.02fms".format((System.nanoTime - s0) / 1000000.0);
    LOG.debug("Shutdown Adapters done @ " + Utils.GetCurDtTmStr + ". " + totaltm)

    true
  }

  private def initialize: Boolean = {
    var retval: Boolean = true

    val loadConfigs = KamanjaConfiguration.allConfigs

    try {
      KamanjaConfiguration.metadataStoreType = loadConfigs.getProperty("MetadataStoreType".toLowerCase, "").replace("\"", "").trim
      if (KamanjaConfiguration.metadataStoreType.size == 0) {
        LOG.error("Not found valid MetadataStoreType.")
        return false
      }

      KamanjaConfiguration.metadataSchemaName = loadConfigs.getProperty("MetadataSchemaName".toLowerCase, "").replace("\"", "").trim
      if (KamanjaConfiguration.metadataSchemaName.size == 0) {
        LOG.error("Not found valid MetadataSchemaName.")
        return false
      }

      KamanjaConfiguration.metadataLocation = loadConfigs.getProperty("MetadataLocation".toLowerCase, "").replace("\"", "").trim
      if (KamanjaConfiguration.metadataLocation.size == 0) {
        LOG.error("Not found valid MetadataLocation.")
        return false
      }

      KamanjaConfiguration.nodeId = loadConfigs.getProperty("nodeId".toLowerCase, "0").replace("\"", "").trim.toInt
      if (KamanjaConfiguration.nodeId <= 0) {
        LOG.error("Not found valid nodeId. It should be greater than 0")
        return false
      }

      // This is just for debugging info. If anything fails here, we should not care.
      try {
        KamanjaConfiguration.waitProcessingTime = loadConfigs.getProperty("waitProcessingTime".toLowerCase, "0").replace("\"", "").trim.toInt
        if (KamanjaConfiguration.waitProcessingTime > 0) {
          val setps = loadConfigs.getProperty("waitProcessingSteps".toLowerCase, "").replace("\"", "").split(",").map(_.trim).filter(_.length() > 0)
          if (setps.size > 0)
            KamanjaConfiguration.waitProcessingSteps = setps.map(_.toInt).toSet
        }
      } catch {
        case e: Exception => {} 
      }

      KamanjaMetadata.InitBootstrap

      if (KamanjaMdCfg.InitConfigInfo == false)
        return false

      var engineLeaderZkNodePath = ""
      var engineDistributionZkNodePath = ""
      var metadataUpdatesZkNodePath = ""
      var adaptersStatusPath = ""
      var dataChangeZkNodePath = ""
      var zkHeartBeatNodePath = ""

      if (KamanjaConfiguration.zkNodeBasePath.size > 0) {
        val zkNodeBasePath = KamanjaConfiguration.zkNodeBasePath.stripSuffix("/").trim
        KamanjaConfiguration.zkNodeBasePath = zkNodeBasePath
        engineLeaderZkNodePath = zkNodeBasePath + "/engineleader"
        engineDistributionZkNodePath = zkNodeBasePath + "/enginedistribution"
        metadataUpdatesZkNodePath = zkNodeBasePath + "/metadataupdate"
        adaptersStatusPath = zkNodeBasePath + "/adaptersstatus"
        dataChangeZkNodePath = zkNodeBasePath + "/datachange"
        zkHeartBeatNodePath = zkNodeBasePath + "/monitor/engine/" + KamanjaConfiguration.nodeId.toString
      }

      KamanjaMdCfg.ValidateAllRequiredJars

      KamanjaMetadata.envCtxt = KamanjaMdCfg.LoadEnvCtxt(metadataLoader)
      if (KamanjaMetadata.envCtxt == null)
        return false

      // Loading Adapters (Do this after loading metadata manager & models & Dimensions (if we are loading them into memory))
      retval = KamanjaMdCfg.LoadAdapters(metadataLoader, inputAdapters, outputAdapters, statusAdapters, validateInputAdapters)

      if (retval) {
        KamanjaMetadata.InitMdMgr(metadataLoader.loadedJars, metadataLoader.loader, metadataLoader.mirror, KamanjaConfiguration.zkConnectString, metadataUpdatesZkNodePath, KamanjaConfiguration.zkSessionTimeoutMs, KamanjaConfiguration.zkConnectionTimeoutMs)
        KamanjaLeader.Init(KamanjaConfiguration.nodeId.toString, KamanjaConfiguration.zkConnectString, engineLeaderZkNodePath, engineDistributionZkNodePath, adaptersStatusPath, inputAdapters, outputAdapters, statusAdapters, validateInputAdapters, KamanjaMetadata.envCtxt, KamanjaConfiguration.zkSessionTimeoutMs, KamanjaConfiguration.zkConnectionTimeoutMs, dataChangeZkNodePath)
      }

      if (retval && zkHeartBeatNodePath.size > 0) {
        heartBeat = new HeartBeatUtil
        heartBeat.Init(KamanjaConfiguration.nodeId.toString, KamanjaConfiguration.zkConnectString, zkHeartBeatNodePath, KamanjaConfiguration.zkSessionTimeoutMs, KamanjaConfiguration.zkConnectionTimeoutMs, 5000) // for every 5 secs
        heartBeat.SetMainData("Node" + KamanjaConfiguration.nodeId.toString)
      }

      /*
      if (retval) {
        try {
          serviceObj = new KamanjaServer(this, KamanjaConfiguration.nodePort)
          (new Thread(serviceObj)).start()
        } catch {
          case e: Exception => {
            LOG.error("Failed to create server to accept connection on port:" + nodePort+ ". Reason:" + e.getCause + ". Message:" + e.getMessage)
            retval = false
          }
        }
      }
*/

    } catch {
      case e: Exception => {
        LOG.error("Failed to initialize. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        // LOG.debug("Failed to initialize. Message:" + e.getMessage + "\n" + e.printStackTrace)
        retval = false
      }
    } finally {

    }

    return retval
  }

  def execCmd(ln: String): Boolean = {
    if (ln.length() > 0) {
      val trmln = ln.trim
      if (trmln.length() > 0 && (trmln.compareToIgnoreCase("Quit") == 0 || trmln.compareToIgnoreCase("Exit") == 0))
        return true
    }
    return false;
  }

  def run(args: Array[String]): Int = {
    KamanjaConfiguration.Reset
    KamanjaLeader.Reset
    if (args.length == 0) {
      PrintUsage()
      return Shutdown(1)
    }

    val options = nextOption(Map(), args.toList)
    val cfgfile = options.getOrElse('config, null)
    if (cfgfile == null) {
      LOG.error("Need configuration file as parameter")
      return Shutdown(1)
    }

    KamanjaConfiguration.configFile = cfgfile.toString
    val (loadConfigs, failStr) = Utils.loadConfiguration(KamanjaConfiguration.configFile, true)
    if (failStr != null && failStr.size > 0) {
      LOG.error(failStr)
      return Shutdown(1)
    }
    if (loadConfigs == null) {
      return Shutdown(1)
    }

    KamanjaConfiguration.allConfigs = loadConfigs

    {
      // Printing all configuration
      LOG.info("Configurations:")
      val it = loadConfigs.entrySet().iterator()
      val lowercaseconfigs = new Properties()
      while (it.hasNext()) {
        val entry = it.next();
        LOG.info("\t" + entry.getKey().asInstanceOf[String] + " -> " + entry.getValue().asInstanceOf[String])
      }
      LOG.info("\n")
    }

    if (LoadDynamicJarsIfRequired(loadConfigs) == false) {
      return Shutdown(1)
    }

    if (initialize == false) {
      return Shutdown(1)
    }

    val statusPrint_PD = new Runnable {
      def run() {
        val stats: scala.collection.immutable.Map[String, Long] = SimpleStats.copyMap
        val statsStr = stats.mkString("~")
        val dispStr = "PD,%d,%s,%s".format(KamanjaConfiguration.nodeId, Utils.GetCurDtTmStr, statsStr)

        if (statusAdapters != null) {
          statusAdapters.foreach(sa => {
            sa.send(dispStr, "1")
          })
        } else {
          LOG.info(dispStr)
        }
      }
    }

    val scheduledThreadPool = Executors.newScheduledThreadPool(2);

    scheduledThreadPool.scheduleWithFixedDelay(statusPrint_PD, 0, 1000, TimeUnit.MILLISECONDS);

    /**
     * print("=> ")
     * breakable {
     * for (ln <- io.Source.stdin.getLines) {
     * val rv = execCmd(ln)
     * if (rv)
     * break;
     * print("=> ")
     * }
     * }
     */

    var timeOutEndTime: Long = 0
    var participentsChangedCntr: Long = 0
    var lookingForDups = false
    var cntr: Long = 0
    var prevParticipents = ""

    val nodeNameToSetZk = "Node" + KamanjaConfiguration.nodeId.toString

    print("KamanjaManager is running now. Waiting for user to terminate with CTRL + C")
    while (KamanjaConfiguration.shutdown == false) { // Infinite wait for now 
      cntr = cntr + 1
      if (participentsChangedCntr != KamanjaConfiguration.participentsChangedCntr) {
        val dispWarn = (lookingForDups && timeOutEndTime > 0)
        lookingForDups = false
        timeOutEndTime = 0
        participentsChangedCntr = KamanjaConfiguration.participentsChangedCntr
        val cs = KamanjaLeader.GetClusterStatus
        if (cs.leader != null && cs.participants != null && cs.participants.size > 0) {
          if (dispWarn) {
            LOG.warn("Got new participents. Trying to see whether the node still has duplicates participents. Previous Participents:{%s} Current Participents:{%s}".format(prevParticipents, cs.participants.mkString(",")))
          }
          prevParticipents = ""
          val isNotLeader = (cs.isLeader == false || cs.leader != cs.nodeId)
          if (isNotLeader) {
            val sameNodeIds = cs.participants.filter(p => p == cs.nodeId)
            if (sameNodeIds.size > 1) {
              lookingForDups = true
              var mxTm = if (KamanjaConfiguration.zkSessionTimeoutMs > KamanjaConfiguration.zkConnectionTimeoutMs) KamanjaConfiguration.zkSessionTimeoutMs else KamanjaConfiguration.zkConnectionTimeoutMs
              if (mxTm < 5000) // if the value is < 5secs, we are taking 5 secs
                mxTm = 5000
              timeOutEndTime = System.currentTimeMillis + mxTm + 2000 // waiting another 2secs
              LOG.error("Found more than one of NodeId:%s in Participents:{%s}. Waiting for %d milli seconds to check whether it is real duplicate or not.".format(cs.nodeId, cs.participants.mkString(","), mxTm))
              prevParticipents = cs.participants.mkString(",")
            }
          }
        }
      }

      if (lookingForDups && timeOutEndTime > 0) {
        if (timeOutEndTime < System.currentTimeMillis) {
          lookingForDups = false
          timeOutEndTime = 0
          val cs = KamanjaLeader.GetClusterStatus
          if (cs.leader != null && cs.participants != null && cs.participants.size > 0) {
            val isNotLeader = (cs.isLeader == false || cs.leader != cs.nodeId)
            if (isNotLeader) {
              val sameNodeIds = cs.participants.filter(p => p == cs.nodeId)
              if (sameNodeIds.size > 1) {
                LOG.error("Found more than one of NodeId:%s in Participents:{%s} for ever. Shutting down this node.".format(cs.nodeId, cs.participants.mkString(",")))
                KamanjaConfiguration.shutdown = true
              }
            }
          }
        }
      }

      try {
        Thread.sleep(500) // Waiting for 500 milli secs
      } catch {
        case e: Exception => {
        }
      }
      if (heartBeat != null && (cntr % 2 == 1)) {
        heartBeat.SetMainData(nodeNameToSetZk)
      }
    }

    scheduledThreadPool.shutdownNow()
    return Shutdown(0)
  }

}

object OleService {
  def main(args: Array[String]): Unit = {
    val mgr = new KamanjaManager
    sys.exit(mgr.run(args))
  }
}

