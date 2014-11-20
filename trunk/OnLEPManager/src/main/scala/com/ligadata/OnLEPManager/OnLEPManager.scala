
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
      case e: Exception => { LOG.error("Socket Error: " + e.getMessage) }
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
      case e: Exception => { LOG.error("Error: " + e.getMessage) }
    } finally {
      socket.close;
    }
  }
}

object OnLEPConfiguration {
  var allConfigs: Properties = _
  var metadataStoreType: String = _
  var metadataSchemaName: String = _
  var metadataLocation: String = _
  var dataStoreType: String = _
  var dataSchemaName: String = _
  var dataLocation: String = _
  var jarPaths: collection.immutable.Set[String] = _
  var nodeId: Int = _
  var zkConnectString: String = _
  var znodePath: String = _

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

  private type OptionMap = Map[Symbol, Any]

  private def PrintUsage(): Unit = {
    LOG.warn("Available commands:")
    LOG.warn("    Quit")
    LOG.warn("    Help")
    LOG.warn("    --config <configfilename>")
  }

  private def Shutdown(exitCode:Int): Unit = {
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

  private def CreateInputAdapterFromConfig(statusAdapterCfg: AdapterConfiguration, outputAdapters: Array[OutputAdapter], envCtxt: EnvContext, loaderInfo: OnLEPLoaderInfo): InputAdapter = {
    if (statusAdapterCfg == null) return null
    var allJars: collection.immutable.Set[String] = null

    if (statusAdapterCfg.dependencyJars != null && statusAdapterCfg.jarName != null) {
      allJars = statusAdapterCfg.dependencyJars + statusAdapterCfg.jarName
    } else if (statusAdapterCfg.dependencyJars != null) {
      allJars = statusAdapterCfg.dependencyJars
    } else if (statusAdapterCfg.jarName != null) {
      allJars = collection.immutable.Set(statusAdapterCfg.jarName)
    }

    if (allJars != null) {
      if (ManagerUtils.LoadJars(allJars.map(j => OnLEPConfiguration.GetValidJarFile(OnLEPConfiguration.jarPaths, j)).toArray, loaderInfo.loadedJars, loaderInfo.loader) == false)
        throw new Exception("Failed to add Jars")
    }

    // Convert class name into a class
    val clz = Class.forName(statusAdapterCfg.className, true, loaderInfo.loader)

    var isInputAdapter = false
    var curClz = clz

    while (clz != null && isInputAdapter == false) {
      isInputAdapter = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.OnLEPBase.InputAdapterObj")
      if (isInputAdapter == false)
        curClz = curClz.getSuperclass()
    }

    if (isInputAdapter) {
      try {
        val module = loaderInfo.mirror.staticModule(statusAdapterCfg.className)
        val obj = loaderInfo.mirror.reflectModule(module)

        val objinst = obj.instance
        if (objinst.isInstanceOf[InputAdapterObj]) {
          val adapterObj = objinst.asInstanceOf[InputAdapterObj]
          val adapter = adapterObj.CreateInputAdapter(statusAdapterCfg, outputAdapters, envCtxt, MakeExecContextImpl, SimpleStats)
          LOG.info("Created Input Adapter for Name:" + statusAdapterCfg.Name + ", Class:" + statusAdapterCfg.className)
          return adapter
        } else {
          LOG.error("Failed to instantiate input adapter object:" + statusAdapterCfg.className)
        }
      } catch {
        case e: Exception => LOG.error("Failed to instantiate input adapter object:" + statusAdapterCfg.className + ". Message:" + e.getMessage)
      }
    } else {
      LOG.error("Failed to instantiate input adapter object:" + statusAdapterCfg.className)
    }
    null
  }

  private def CreateOutputAdapterFromConfig(statusAdapterCfg: AdapterConfiguration, loaderInfo: OnLEPLoaderInfo): OutputAdapter = {
    if (statusAdapterCfg == null) return null
    var allJars: collection.immutable.Set[String] = null
    if (statusAdapterCfg.dependencyJars != null && statusAdapterCfg.jarName != null) {
      allJars = statusAdapterCfg.dependencyJars + statusAdapterCfg.jarName
    } else if (statusAdapterCfg.dependencyJars != null) {
      allJars = statusAdapterCfg.dependencyJars
    } else if (statusAdapterCfg.jarName != null) {
      allJars = collection.immutable.Set(statusAdapterCfg.jarName)
    }

    if (allJars != null) {
      if (ManagerUtils.LoadJars(allJars.map(j => OnLEPConfiguration.GetValidJarFile(OnLEPConfiguration.jarPaths, j)).toArray, loaderInfo.loadedJars, loaderInfo.loader) == false)
        throw new Exception("Failed to add Jars")
    }

    // Convert class name into a class
    val clz = Class.forName(statusAdapterCfg.className, true, loaderInfo.loader)

    var isOutputAdapter = false
    var curClz = clz

    while (clz != null && isOutputAdapter == false) {
      isOutputAdapter = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.OnLEPBase.OutputAdapterObj")
      if (isOutputAdapter == false)
        curClz = curClz.getSuperclass()
    }

    if (isOutputAdapter) {
      try {
        val module = loaderInfo.mirror.staticModule(statusAdapterCfg.className)
        val obj = loaderInfo.mirror.reflectModule(module)

        val objinst = obj.instance
        if (objinst.isInstanceOf[OutputAdapterObj]) {
          val adapterObj = objinst.asInstanceOf[OutputAdapterObj]
          val adapter = adapterObj.CreateOutputAdapter(statusAdapterCfg, SimpleStats)
          LOG.info("Created Output Adapter for Name:" + statusAdapterCfg.Name + ", Class:" + statusAdapterCfg.className)
          return adapter
        } else {
          LOG.error("Failed to instantiate output adapter object:" + statusAdapterCfg.className)
        }
      } catch {
        case e: Exception => LOG.error("Failed to instantiate output adapter object:" + statusAdapterCfg.className + ". Message:" + e.getMessage)
      }
    } else {
      LOG.error("Failed to instantiate output adapter object:" + statusAdapterCfg.className)
    }
    null
  }

  private def GetAdapterConfigByCfgName(loadConfigs: Properties, adapCfgName: String): AdapterConfiguration = {
    // For any adapter we need Type as first one
    val adapterConfig = loadConfigs.getProperty(adapCfgName.toLowerCase, "").replace("\"", "").trim.split("~", -1).map(str => str.trim)

    if (adapterConfig.size == 0) {
      LOG.error("Not found Type for Adapter Config:" + adapCfgName)
      return null
    }

    //BUGBUG:: Not yet validating required fields 
    val conf = new AdapterConfiguration

    conf.Typ = adapterConfig(0)
    conf.Name = adapterConfig(1)
    conf.className = adapterConfig(2)
    conf.jarName = adapterConfig(3)
    conf.dependencyJars = if (adapterConfig(4).size > 0) adapterConfig(4).split(",").map(str => str.trim).filter(str => str.size > 0).toSet else null

    if (adapterConfig.size > 5) {
      val remVals = adapterConfig.size - 5
      conf.adapterSpecificTokens = new Array(remVals)

      for (i <- 0 until remVals)
        conf.adapterSpecificTokens(i) = adapterConfig(i + 5)
    }

    conf
  }

  private def LoadInputAdapsForCfg(loadConfigs: Properties, cfgNames: String, mustHave: Boolean, inputAdapters: ArrayBuffer[InputAdapter], outputAdapters: Array[OutputAdapter], envCtxt: EnvContext, loaderInfo: OnLEPLoaderInfo): Boolean = {
    // ConfigurationName
    val adapCfgNames = loadConfigs.getProperty(cfgNames.toLowerCase, "").replace("\"", "").trim.split("~").map(str => str.trim).filter(str => str.size > 0)

    if (adapCfgNames.size == 0) {
      if (mustHave) {
        LOG.error("Not found valid %s ConfigurationNames".format(cfgNames))
        return false
      } else {
        LOG.warn("Not found valid %s ConfigurationNames".format(cfgNames))
        return true
      }
    } else {
      adapCfgNames.foreach(ac => {
        val adapterCfg = GetAdapterConfigByCfgName(loadConfigs, ac)
        if (adapterCfg == null) return false

        try {
          val adapter = CreateInputAdapterFromConfig(adapterCfg, outputAdapters, envCtxt, loaderInfo)
          if (adapter == null) return false
          inputAdapters += adapter
        } catch {
          case e: Exception =>
            LOG.error("Failed to get input adapter for %s. Message:%s".format(ac, e.getMessage))
            return false
        }
      })
      return true
    }
  }

  private def LoadOutputAdapsForCfg(loadConfigs: Properties, cfgNames: String, mustHave: Boolean, outputAdapters: ArrayBuffer[OutputAdapter], loaderInfo: OnLEPLoaderInfo): Boolean = {
    // ConfigurationName
    val adapCfgNames = loadConfigs.getProperty(cfgNames.toLowerCase, "").replace("\"", "").trim.split("~").map(str => str.trim).filter(str => str.size > 0)

    if (adapCfgNames.size == 0) {
      if (mustHave) {
        LOG.error("Not found valid %s ConfigurationNames".format(cfgNames))
        return false
      } else {
        LOG.warn("Not found valid %s ConfigurationNames".format(cfgNames))
        return true
      }
    } else {
      adapCfgNames.foreach(ac => {
        val adapterCfg = GetAdapterConfigByCfgName(loadConfigs, ac)
        if (adapterCfg == null) return false

        try {
          val adapter = CreateOutputAdapterFromConfig(adapterCfg, loaderInfo)
          if (adapter == null) return false
          outputAdapters += adapter
        } catch {
          case e: Exception =>
            LOG.error("Failed to get output adapter for %s. Message:%s".format(ac, e.getMessage))
            return false
        }
      })
      return true
    }
  }

  private def LoadEnvCtxt(loadConfigs: Properties, loaderInfo: OnLEPLoaderInfo): EnvContext = {
    val envCxtConfig = loadConfigs.getProperty("EnvironmentContext".toLowerCase, "").replace("\"", "").trim.split("~", -1).map(str => str.trim)

    if (envCxtConfig.size != 3) {
      LOG.error("EnvironmentContext configuration need ClassName~JarName~DependencyJars")
      return null
    }

    //BUGBUG:: Not yet validating required fields 
    val className = envCxtConfig(0)
    val jarName = envCxtConfig(1)
    val dependencyJars = if (envCxtConfig(2).size > 0) envCxtConfig(2).split(",").map(str => str.trim).filter(str => str.size > 0).toSet else null
    var allJars: collection.immutable.Set[String] = null

    if (dependencyJars != null && jarName != null) {
      allJars = dependencyJars + jarName
    } else if (dependencyJars != null) {
      allJars = dependencyJars
    } else if (jarName != null) {
      allJars = collection.immutable.Set(jarName)
    }

    if (allJars != null) {
      if (ManagerUtils.LoadJars(allJars.map(j => OnLEPConfiguration.GetValidJarFile(OnLEPConfiguration.jarPaths, j)).toArray, loaderInfo.loadedJars, loaderInfo.loader) == false)
        throw new Exception("Failed to add Jars")
    }

    // Convert class name into a class
    val clz = Class.forName(className, true, loaderInfo.loader)

    var isEntCtxt = false
    var curClz = clz

    while (clz != null && isEntCtxt == false) {
      isEntCtxt = ManagerUtils.isDerivedFrom(curClz, "com.ligadata.OnLEPBase.EnvContext")
      if (isEntCtxt == false)
        curClz = curClz.getSuperclass()
    }

    if (isEntCtxt) {
      try {
        val module = loaderInfo.mirror.staticModule(className)
        val obj = loaderInfo.mirror.reflectModule(module)

        val objinst = obj.instance
        if (objinst.isInstanceOf[EnvContext]) {
          val envCtxt = objinst.asInstanceOf[EnvContext]
          envCtxt.SetClassLoader(metadataLoader.loader)
          val containerNames = OnLEPMetadata.getAllContainers.map(container => container._1.toLowerCase).toList.sorted.toArray // Sort topics by names
          val topMessageNames = OnLEPMetadata.getAllMessges.filter(msg => msg._2.parents.size == 0).map(msg => msg._1.toLowerCase).toList.sorted.toArray // Sort topics by names
          envCtxt.AddNewMessageOrContainers(OnLEPMetadata.getMdMgr, OnLEPConfiguration.dataStoreType, OnLEPConfiguration.dataLocation, OnLEPConfiguration.dataSchemaName, containerNames, true) // Containers
          envCtxt.AddNewMessageOrContainers(OnLEPMetadata.getMdMgr, OnLEPConfiguration.dataStoreType, OnLEPConfiguration.dataLocation, OnLEPConfiguration.dataSchemaName, topMessageNames, false) // Messages
          LOG.info("Created EnvironmentContext for Class:" + className)
          return envCtxt
        } else {
          LOG.error("Failed to instantiate Environment Context object for Class:" + className + ". ObjType0:" + objinst.getClass.getSimpleName + ". ObjType1:" + objinst.getClass.getCanonicalName)
        }
      } catch {
        case e: Exception => {
          LOG.error("Failed to instantiate Environment Context object for Class:" + className + ". Message:" + e.getMessage)
          e.printStackTrace
        }
      }
    } else {
      LOG.error("Failed to instantiate Environment Context object for Class:" + className)
    }
    null
  }

  private def LoadAdapters(loadConfigs: Properties, envCtxt: EnvContext): Boolean = {
    LOG.info("Loading Adapters started @ " + Utils.GetCurDtTmStr)
    val s0 = System.nanoTime

    // Get status adapter
    LOG.info("Getting Status Adapter")

    if (LoadOutputAdapsForCfg(loadConfigs, "StatusAdapterCfgNames", false, statusAdapters, metadataLoader) == false)
      return false

    // Get output adapter
    LOG.info("Getting Output Adapters")

    if (LoadOutputAdapsForCfg(loadConfigs, "OutputAdapterCfgNames", false, outputAdapters, metadataLoader) == false)
      return false

    // Get input adapter
    LOG.info("Getting Input Adapters")

    if (LoadInputAdapsForCfg(loadConfigs, "InputAdapterCfgNames", false, inputAdapters, outputAdapters.toArray, envCtxt, metadataLoader) == false)
      return false

    val totaltm = "TimeConsumed:%.02fms".format((System.nanoTime - s0) / 1000000.0);
    LOG.info("Loading Adapters done @ " + Utils.GetCurDtTmStr + totaltm)

    true
  }

  private def ShutdownAdapters: Boolean = {
    LOG.info("Shutdown Adapters started @ " + Utils.GetCurDtTmStr)
    val s0 = System.nanoTime

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
      OnLEPConfiguration.jarPaths = loadConfigs.getProperty("JarPaths".toLowerCase, "").replace("\"", "").trim.split(",").map(str => str.trim).filter(str => str.size > 0).toSet
      if (OnLEPConfiguration.jarPaths.size == 0) {
        OnLEPConfiguration.jarPaths = loadConfigs.getProperty("JarPath".toLowerCase, "").replace("\"", "").trim.split(",").map(str => str.trim).filter(str => str.size > 0).toSet
        if (OnLEPConfiguration.jarPaths.size == 0) {
          LOG.error("Not found valid JarPaths.")
          return false
        }
      }

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

      OnLEPConfiguration.dataStoreType = loadConfigs.getProperty("DataStoreType".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.dataStoreType.size == 0) {
        LOG.error("Not found valid DataStoreType.")
        return false
      }

      OnLEPConfiguration.dataSchemaName = loadConfigs.getProperty("DataSchemaName".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.dataSchemaName.size == 0) {
        LOG.error("Not found valid DataSchemaName.")
        return false
      }

      OnLEPConfiguration.dataLocation = loadConfigs.getProperty("DataLocation".toLowerCase, "").replace("\"", "").trim
      if (OnLEPConfiguration.dataLocation.size == 0) {
        LOG.error("Not found valid DataLocation.")
        return false
      }

      OnLEPConfiguration.nodeId = loadConfigs.getProperty("nodeId".toLowerCase, "0").replace("\"", "").trim.toInt
      if (OnLEPConfiguration.nodeId <= 0) {
        LOG.error("Not found valid nodeId. It should be greater than 0")
        return false
      }

      OnLEPConfiguration.zkConnectString = loadConfigs.getProperty("ZooKeeperConnectString".toLowerCase, "").replace("\"", "").trim
      OnLEPConfiguration.znodePath = loadConfigs.getProperty("ZnodePath".toLowerCase, "").replace("\"", "").trim

      val nodePort: Int = loadConfigs.getProperty("nodePort".toLowerCase, "0").replace("\"", "").trim.toInt
      if (nodePort <= 0) {
        LOG.error("Not found valid nodePort. It should be greater than 0")
        return false
      }

      OnLEPMetadata.InitMdMgr(metadataLoader.loadedJars, metadataLoader.loader, metadataLoader.mirror, OnLEPConfiguration.zkConnectString, OnLEPConfiguration.znodePath)

      val envCtxt = LoadEnvCtxt(loadConfigs, metadataLoader)
      if (envCtxt == null)
        return false

      OnLEPMetadata.envCtxt = envCtxt

      // Loading Adapters (Do this after loading metadata manager & models & Dimensions (if we are loading them into memory))
      retval = LoadAdapters(loadConfigs, envCtxt)

      if (retval) {
        try {
          serviceObj = new OnLEPServer(this, nodePort)
          (new Thread(serviceObj)).start()
        } catch {
          case e: Exception => {
            LOG.error("Failed to create server to accept connection on port:" + nodePort + ". Message:" + e.getMessage)
            retval = false
          }
        }
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to initialize. Message:" + e.getMessage)
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

    val (loadConfigs, failStr) = Utils.loadConfiguration(cfgfile.toString, true)
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

