
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase._

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

class OnLEPServer(var mgr: OnLEPManager, port: Int) extends Runnable {
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

class ConnHandler(var socket: Socket, var mgr: OnLEPManager) extends Runnable {
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

object OnLEPConfiguration {
  var configFile: String = _
  var allConfigs: Properties = _
  var metadataStoreType: String = _
  var metadataSchemaName: String = _
  var metadataLocation: String = _
  var dataStoreType: String = _
  var dataSchemaName: String = _
  var dataLocation: String = _
  var statusInfoStoreType: String = _
  var statusInfoSchemaName: String = _
  var statusInfoLocation: String = _
  var jarPaths: collection.immutable.Set[String] = _
  var nodeId: Int = _
  var clusterId: String = _
  var nodePort: Int = _
  var zkConnectString: String = _
  var zkNodeBasePath: String = _
  var zkSessionTimeoutMs: Int = _
  var zkConnectionTimeoutMs: Int = _

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
}

class OnLEPClassLoader(urls: Array[URL], parent: ClassLoader) extends URLClassLoader(urls, parent) {
  override def addURL(url: URL) {
    super.addURL(url)
  }
}

class OnLEPLoaderInfo {
  // class loader
  val loader: OnLEPClassLoader = new OnLEPClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader].getURLs(), getClass().getClassLoader())

  // Loaded jars
  val loadedJars: TreeSet[String] = new TreeSet[String];

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)

  // ru.runtimeMirror(modelsloader)
}

class OnLEPManager {
  private val LOG = Logger.getLogger(getClass);

  // metadata loader
  private val metadataLoader = new OnLEPLoaderInfo

  // OnLEPServer Object
  private var serviceObj: OnLEPServer = null

  private val inputAdapters = new ArrayBuffer[InputAdapter]
  private val outputAdapters = new ArrayBuffer[OutputAdapter]
  private val statusAdapters = new ArrayBuffer[OutputAdapter]
  private val validateInputAdapters = new ArrayBuffer[InputAdapter]

  private type OptionMap = Map[Symbol, Any]

  private def PrintUsage(): Unit = {
    LOG.warn("Available commands:")
    LOG.warn("    Quit")
    LOG.warn("    Help")
    LOG.warn("    --config <configfilename>")
  }

  private def Shutdown(exitCode: Int): Unit = {
    OnLEPLeader.Shutdown
    OnLEPMetadata.Shutdown
    ShutdownAdapters
    if (OnLEPMetadata.envCtxt != null)
      OnLEPMetadata.envCtxt.Shutdown
    if (serviceObj != null)
      serviceObj.shutdown
    sys.exit(exitCode)
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        LOG.error("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  private def LoadDynamicJarsIfRequired(loadConfigs: Properties): Boolean = {
    val dynamicjars: String = loadConfigs.getProperty("dynamicjars".toLowerCase, "").trim

    if (dynamicjars != null && dynamicjars.length() > 0) {
      val jars = dynamicjars.split(",").map(_.trim).filter(_.length() > 0)
      if (jars.length > 0)
        return ManagerUtils.LoadJars(jars, metadataLoader.loadedJars, metadataLoader.loader)
    }

    true
  }

  private def ShutdownAdapters: Boolean = {
    LOG.info("Shutdown Adapters started @ " + Utils.GetCurDtTmStr)
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
    LOG.info("Shutdown Adapters done @ " + Utils.GetCurDtTmStr + ". " + totaltm)

    true
  }

  private def initialize: Boolean = {
    var retval: Boolean = true

    val loadConfigs = OnLEPConfiguration.allConfigs

    try {
      OnLEPConfiguration.metadataStoreType = loadConfigs.getProperty("MetadataStoreType".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.metadataStoreType.size == 0) {
        LOG.error("Not found valid MetadataStoreType.")
        return false
      }

      OnLEPConfiguration.metadataSchemaName = loadConfigs.getProperty("MetadataSchemaName".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.metadataSchemaName.size == 0) {
        LOG.error("Not found valid MetadataSchemaName.")
        return false
      }

      OnLEPConfiguration.metadataLocation = loadConfigs.getProperty("MetadataLocation".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.metadataLocation.size == 0) {
        LOG.error("Not found valid MetadataLocation.")
        return false
      }

      OnLEPConfiguration.nodeId = loadConfigs.getProperty("nodeId".toLowerCase, "0").replace("\"", "").trim.toInt
      if (OnLEPConfiguration.nodeId <= 0) {
        LOG.error("Not found valid nodeId. It should be greater than 0")
        return false
      }

      OnLEPMetadata.InitBootstrap

      if (OnLEPMdCfg.InitConfigInfo == false)
        return false

      var engineLeaderZkNodePath = ""
      var engineDistributionZkNodePath = ""
      var metadataUpdatesZkNodePath = ""
      var adaptersStatusPath = ""

      if (OnLEPConfiguration.zkNodeBasePath.size > 0) {
        val zkNodeBasePath = OnLEPConfiguration.zkNodeBasePath.stripSuffix("/").trim
        engineLeaderZkNodePath = zkNodeBasePath + "/engineleader"
        engineDistributionZkNodePath = zkNodeBasePath + "/enginedistribution"
        metadataUpdatesZkNodePath = zkNodeBasePath + "/metadataupdate"
        adaptersStatusPath = zkNodeBasePath + "/adaptersstatus"
      }

      OnLEPMetadata.envCtxt = OnLEPMdCfg.LoadEnvCtxt(metadataLoader)
      if (OnLEPMetadata.envCtxt == null)
        return false

      // Loading Adapters (Do this after loading metadata manager & models & Dimensions (if we are loading them into memory))
      retval = OnLEPMdCfg.LoadAdapters(metadataLoader, inputAdapters, outputAdapters, statusAdapters, validateInputAdapters)

      if (retval) {
        OnLEPMetadata.InitMdMgr(metadataLoader.loadedJars, metadataLoader.loader, metadataLoader.mirror, OnLEPConfiguration.zkConnectString, metadataUpdatesZkNodePath, OnLEPConfiguration.zkSessionTimeoutMs, OnLEPConfiguration.zkConnectionTimeoutMs)
        OnLEPLeader.Init(OnLEPConfiguration.nodeId.toString, OnLEPConfiguration.zkConnectString, engineLeaderZkNodePath, engineDistributionZkNodePath, adaptersStatusPath, inputAdapters, outputAdapters, statusAdapters, validateInputAdapters, OnLEPMetadata.envCtxt, OnLEPConfiguration.zkSessionTimeoutMs, OnLEPConfiguration.zkConnectionTimeoutMs)
      }

      /*
      if (retval) {
        try {
          serviceObj = new OnLEPServer(this, OnLEPConfiguration.nodePort)
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
        // LOG.info("Failed to initialize. Message:" + e.getMessage + "\n" + e.printStackTrace)
        retval = false
      }
    } finally {

    }

    return retval
  }

  def execCmd(ln: String): Boolean = {
    if (ln.length() > 0) {
      if (ln.compareToIgnoreCase("Quit") == 0)
        return true
    }
    return false;
  }

  def run(args: Array[String]): Unit = {
    if (args.length == 0) {
      PrintUsage()
      Shutdown(1)
      return
    }

    val options = nextOption(Map(), args.toList)
    val cfgfile = options.getOrElse('config, null)
    if (cfgfile == null) {
      LOG.error("Need configuration file as parameter")
      Shutdown(1)
      return
    }

    OnLEPConfiguration.configFile = cfgfile.toString
    val (loadConfigs, failStr) = Utils.loadConfiguration(OnLEPConfiguration.configFile, true)
    if (failStr != null && failStr.size > 0) {
      LOG.error(failStr)
      Shutdown(1)
      return
    }
    if (loadConfigs == null) {
      Shutdown(1)
      return
    }

    OnLEPConfiguration.allConfigs = loadConfigs

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
      Shutdown(1)
      return
    }

    /*
    if (initialize == false) {
      Shutdown(1)
      return
    }
*/

    if (initialize == false) {
      Shutdown(1)
      return
    }

    val statusPrint_PD = new Runnable {
      def run() {
        val stats: scala.collection.immutable.Map[String, Long] = SimpleStats.copyMap
        val statsStr = stats.mkString("~")
        val dispStr = "PD,%d,%s,%s".format(OnLEPConfiguration.nodeId, Utils.GetCurDtTmStr, statsStr)

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

    print("=> ")
    breakable {
      for (ln <- io.Source.stdin.getLines) {
        val rv = execCmd(ln)
        if (rv)
          break;
        print("=> ")
      }
    }
    scheduledThreadPool.shutdownNow()
    Shutdown(0)
  }

}

object OleService {
  def main(args: Array[String]): Unit = {
    val mgr = new OnLEPManager
    mgr.run(args)
  }
}

