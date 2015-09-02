package com.ligadata.WriteMdlsDataToFileFromKafka;

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import java.io.{ OutputStream, FileOutputStream, File, BufferedWriter, Writer, PrintWriter }
import java.util.zip.GZIPOutputStream
import java.nio.file.{ Paths, Files }
import com.ligadata.Exceptions.StackTrace
import com.ligadata.Utils.Utils
import java.util.concurrent.atomic.AtomicInteger
import java.util.{ Observer, Observable }
import com.ligadata.InputOutputAdapterInfo._
import com.ligadata.InputAdapters._
import com.ligadata.AdaptersConfiguration.{ KafkaPartitionUniqueRecordKey, KafkaPartitionUniqueRecordValue }
// import org.json4s._
import org.json4s.jackson.JsonMethods._

object SimpleStats extends CountersAdapter {
  override def addCntr(key: String, cnt: Long): Long = 0
  override def addCntr(cntrs: scala.collection.immutable.Map[String, Long]): Unit = {}
  override def getCntr(key: String): Long = 0
  override def getDispString(delim: String): String = ""
  override def copyMap: scala.collection.immutable.Map[String, Long] = null
}

object ExecContextObjImpl extends ExecContextObj {
  def CreateExecContext(input: InputAdapter, curPartitionKey: PartitionUniqueRecordKey, callerCtxt: InputAdapterCallerContext): ExecContext = {
    new ExecContextImpl(input, curPartitionKey, callerCtxt)
  }
}

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ExecContextImpl(val input: InputAdapter, val curPartitionKey: PartitionUniqueRecordKey, val callerCtxt: InputAdapterCallerContext) extends ExecContext {
  private val LOG = Logger.getLogger(getClass);

  def execute(data: Array[Byte], format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, associatedMsg: String, delimiterString: String): Unit = {
    if (format.equalsIgnoreCase("json")) {
      WriteMdlsDataToFileFromKafka.processMdlResult(new String(data), uniqueKey, uniqueVal)
    } // else not handling at this moment
  }
}

object WriteMdlsDataToFileFromKafka extends Observer {
  class SignalHandler extends Observable with sun.misc.SignalHandler {
    def handleSignal(signalName: String) {
      sun.misc.Signal.handle(new sun.misc.Signal(signalName), this)
    }
    def handle(signal: sun.misc.Signal) {
      setChanged()
      notifyObservers(signal)
    }
  }

  class OutputModelConfig {
    var ModelName: String = null
    var Format: String = "json"
    var Fields: Array[String] = null
    var WriteHeader: Boolean = false
    var FieldDelimiter: String = ","
    var RecordDelimiter: String = "\n"
    var Compression: String = null
    var FileName: String = null
    var CanAppend: Boolean = false
  }

  class InputKafkaConfig {
    var HostList: String = null
    var Topic: String = null
    var Format: String = "json"
    var PartitionStartOffsets: Array[(Int, Int)] = null // PartitionId & Offset
  }

  class OutputMdlsInfo {
    var ModelName: String = null
    var cfg: OutputModelConfig = null
    var os: OutputStream = null
  }

  private val outMdlsInfoMap = scala.collection.mutable.Map[String, ArrayBuffer[OutputMdlsInfo]]() // Model Name & List of different configs for same Model
  private val inputKafkaAdapters = ArrayBuffer[InputAdapter]()
  private val ProcessedOffsets = scala.collection.mutable.Map[String, scala.collection.mutable.Map[Int, Long]]()
  private val NameToKafkaAdapterInfo = scala.collection.mutable.Map[String, InputKafkaConfig]()

  private val LOG = Logger.getLogger(getClass);

  private type OptionMap = Map[Symbol, Any]

  private def PrintUsage(): Unit = {
    LOG.warn("Available commands:")
    LOG.warn("    --config <configfilename>")
  }

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail =>
        nextOption(map ++ Map('config -> value), tail)
      case option :: tail => {
        LOG.error("%s:Unknown option:%s".format(Utils.GetCurDtTmStr, option))
        sys.exit(1)
      }
    }
  }

  private def ProcessInputConfig(inputConfig: Any): Array[InputKafkaConfig] = {
    if (inputConfig == null)
      return Array[InputKafkaConfig]()

    val kafkaCfgList = if (inputConfig.isInstanceOf[List[Any]]) inputConfig.asInstanceOf[List[Any]].toArray else if (inputConfig.isInstanceOf[Array[Any]]) inputConfig.asInstanceOf[Array[Any]] else null

    if (kafkaCfgList == null) {
      val msg = "%s:Expecting input configuration as list of kafka configurations".format(Utils.GetCurDtTmStr)
      LOG.error(msg)
      throw new Exception(msg)
    }

    val cfgs = kafkaCfgList.map(kafkaCfg => {
      val cfg = new InputKafkaConfig

      val tmpmap = kafkaCfg.asInstanceOf[Map[String, Any]]

      // Convert all keys to lower case to access the value by name
      val map = tmpmap.map { case (k, v) => (k.toLowerCase, v) }

      cfg.HostList = map.getOrElse("hostlist", "").toString.trim
      cfg.Topic = map.getOrElse("topic", "").toString.trim
      cfg.Format = map.getOrElse("format", "json").toString.trim.toLowerCase
      val partsStartOffs = map.getOrElse("partitionstartoffsets", null)
      if (partsStartOffs != null) {
        val partsList = if (partsStartOffs.isInstanceOf[List[Any]]) partsStartOffs.asInstanceOf[List[Any]].toArray else if (partsStartOffs.isInstanceOf[Array[Any]]) partsStartOffs.asInstanceOf[Array[Any]] else null
        if (partsList != null) {
          val pLists = partsList.map(partoff => {
            val tmap = partoff.asInstanceOf[Map[String, Any]]
            val m1 = tmap.map { case (k, v) => (k.toLowerCase, v) }
            val part = m1.getOrElse("partition", null)
            val off = m1.getOrElse("offset", null)
            (part, off)
          })
          cfg.PartitionStartOffsets = pLists.filter(p => p._1 != null && p._2 != null).map(p => (p._1.toString.trim, p._2.toString.trim)).filter(p => p._1.size > 0 && p._2.size > 0).map(p => (p._1.toInt, p._2.toInt))
        }
      }

      cfg
    })

    cfgs

  }

  private def ProcessOutputConfig(outputConfig: Any): Array[OutputModelConfig] = {
    if (outputConfig == null)
      return Array[OutputModelConfig]()

    val mdlsCfgList = if (outputConfig.isInstanceOf[List[Any]]) outputConfig.asInstanceOf[List[Any]].toArray else if (outputConfig.isInstanceOf[Array[Any]]) outputConfig.asInstanceOf[Array[Any]] else null

    if (mdlsCfgList == null) {
      val msg = "%s:Expecting output configuration as list of models output".format(Utils.GetCurDtTmStr)
      LOG.error(msg)
      throw new Exception(msg)
    }

    val cfgs = mdlsCfgList.map(mdlCfg => {
      val cfg = new OutputModelConfig

      val tmpmap = mdlCfg.asInstanceOf[Map[String, Any]]

      // Convert all keys to lower case to access the value by name
      val map = tmpmap.map { case (k, v) => (k.toLowerCase, v) }

      cfg.ModelName = map.getOrElse("modelname", "").toString.trim.toLowerCase
      cfg.Format = map.getOrElse("format", "json").toString.trim.trim.toLowerCase
      cfg.WriteHeader = map.getOrElse("writeheader", "false").toString.trim.toBoolean
      cfg.FieldDelimiter = map.getOrElse("fielddelimiter", ",").toString
      cfg.RecordDelimiter = map.getOrElse("recorddelimiter", "\n").toString
      cfg.Compression = map.getOrElse("compression", "").toString.trim.toLowerCase
      cfg.FileName = map.getOrElse("filename", "").toString.trim
      cfg.CanAppend = map.getOrElse("append", "false").toString.trim.toBoolean
      val flds = map.getOrElse("fields", null)
      if (flds != null) {
        val fields = if (flds.isInstanceOf[List[Any]]) flds.asInstanceOf[List[String]].toArray else if (flds.isInstanceOf[Array[Any]]) flds.asInstanceOf[Array[String]] else Array[String]()
        cfg.Fields = fields.map(f => f.trim.toLowerCase).filter(f => f.size > 0)
      }
      cfg
    })

    cfgs
  }

  private def ValidateInputConfig(inputConfig: Array[InputKafkaConfig]): Unit = {
    if (inputConfig.size == 0) {
      val msg = "%s:Not found Input in configuration file".format(Utils.GetCurDtTmStr)
      LOG.error(msg)
      throw new Exception(msg)
    }

    inputConfig.foreach(ic => {
      if (ic.HostList == null || ic.HostList.size == 0) {
        val msg = "%s:Not found HostList for some of the Input configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (ic.Topic == null || ic.Topic.size == 0) {
        val msg = "%s:Not found Topic for some of the Input configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (ic.Format == null || ic.Format.size == 0) {
        val msg = "%s:Not found Format for some of the Input configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (ic.Format.compareToIgnoreCase("json") != 0) {
        val msg = "%s:We don't handle other than JSON formain in input".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }
    })
  }

  private def ValidateOutputConfig(outputConfig: Array[OutputModelConfig]): Unit = {
    if (outputConfig.size == 0) {
      val msg = "%s:Not found Output in configuration file".format(Utils.GetCurDtTmStr)
      LOG.error(msg)
      throw new Exception(msg)
    }

    var files = scala.collection.mutable.TreeSet[String]()

    outputConfig.foreach(oc => {
      if (oc.ModelName == null || oc.ModelName.size == 0) {
        val msg = "%s:Not found ModelName for some of the Output configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (oc.Format == null || oc.Format.size == 0) {
        val msg = "%s:Not found Format for some of the Output configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (oc.FileName == null || oc.FileName.size == 0) {
        val msg = "%s:Not found FileName for some of the Output configurations".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (files(oc.FileName)) {
        val msg = "%s:Output FileName:%s is used more than once".format(Utils.GetCurDtTmStr, oc.FileName)
        LOG.error(msg)
        throw new Exception(msg)
      }

      files += oc.FileName

      if (oc.Compression != null && oc.Compression.size > 0 && oc.Compression.compareToIgnoreCase("gz") != 0) {
        val msg = "%s:Not handling Compression other than gz. Ignore this value for non compression file".format(Utils.GetCurDtTmStr)
        LOG.error(msg)
        throw new Exception(msg)
      }

      if (oc.Format.compareToIgnoreCase("delimited") == 0) {
        // May be we can have records in sequence without any record separator (mainly for json we can have it)
        if (oc.RecordDelimiter == null || oc.RecordDelimiter.size == 0) {
          val msg = "%s:Not found RecordDelimiter for some of the Output configurations. We must need this for delimited output.".format(Utils.GetCurDtTmStr)
          LOG.error(msg)
          throw new Exception(msg)
        }

        if (oc.Fields == null || oc.Fields.size == 0) {
          val msg = "%s:Not found Fields for some of the Output configurations".format(Utils.GetCurDtTmStr)
          LOG.error(msg)
          throw new Exception(msg)
        }

        if (oc.FieldDelimiter == null || oc.FieldDelimiter.size == 0) {
          val msg = "%s:Not found FieldDelimiter for some of the Output configurations".format(Utils.GetCurDtTmStr)
          LOG.error(msg)
          throw new Exception(msg)
        }
      }
    })
  }

  private def OpenOutputFiles(outputConfig: Array[OutputModelConfig]): Unit = {
    val errCnt = new AtomicInteger
    val outMdlInfos = outputConfig.map(oc => {
      val outInfo = new OutputMdlsInfo
      outInfo.ModelName = oc.ModelName
      outInfo.cfg = oc
      try {
        if (oc.Compression == null || oc.Compression.size == 0) {
          outInfo.os = new FileOutputStream(oc.FileName, oc.CanAppend);
        } else if (oc.Compression.compareToIgnoreCase("gz") == 0) {
          outInfo.os = new GZIPOutputStream(new FileOutputStream(oc.FileName, oc.CanAppend))
        }
      } catch {
        case e: Exception => {
          outInfo.os = null
          errCnt.incrementAndGet
          LOG.error("%s:Failed to open file. Message:%s, Reason:%s ".format(Utils.GetCurDtTmStr, e.getMessage, e.getCause))
        }
      }
      outInfo
    })

    if (errCnt.get > 0) {
      // Close all opened files and throw an error
      outMdlInfos.foreach(oi => {
        if (oi.os != null)
          try {
            oi.os.close
          } catch {
            case e: Exception => {
            }
          }
      })

      throw new Exception("Failed to open some files")
    }

    val sb = new StringBuilder()

    outMdlInfos.foreach(oi => {
      val mdlNm = oi.ModelName.toLowerCase
      val tmpmdlInfo = outMdlsInfoMap.getOrElse(mdlNm, null)
      val mdlInfo =
        if (tmpmdlInfo != null) {
          tmpmdlInfo
        } else {
          ArrayBuffer[OutputMdlsInfo]()
        }

      if (oi.cfg.WriteHeader && oi.cfg.Format.compareToIgnoreCase("delimited") == 0 && oi.cfg.Fields.size > 0) {
        sb.setLength(0)
        var writtenBefore = false
        oi.cfg.Fields.foreach(f => {
          if (writtenBefore)
            sb.append(oi.cfg.FieldDelimiter)
          sb.append(f)
          writtenBefore = true
        })
        sb.append(oi.cfg.RecordDelimiter)
        oi.cfg.synchronized {
          oi.os.write(sb.toString.getBytes("UTF8"))
        }
      }

      mdlInfo += oi
      outMdlsInfoMap(mdlNm) = mdlInfo
    })

  }

  private def getOffset(PartitionStartOffsets: Array[(Int, Int)], key: PartitionUniqueRecordKey, defaultVal: PartitionUniqueRecordValue): PartitionUniqueRecordValue = {
    if (PartitionStartOffsets == null || PartitionStartOffsets.size == 0)
      return defaultVal

    val kafkaKey = key.asInstanceOf[KafkaPartitionUniqueRecordKey]
    val defVal = defaultVal.asInstanceOf[KafkaPartitionUniqueRecordValue]

    PartitionStartOffsets.foreach(f => {
      if (f._1 == kafkaKey.PartitionId) {
        val v = new KafkaPartitionUniqueRecordValue
        v.Offset = f._2 - 1 // Need to give one less than Start Position
        return v
      }
    })

    defaultVal
  }

  private def StartProcessingInput(inputConfig: Array[InputKafkaConfig]): Unit = {
    var idx = 0
    val adapConfigs = inputConfig.map(ic => {
      val adapConf = new AdapterConfiguration
      adapConf.Name = ic.HostList + "~" + ic.Topic + "~" + idx
      adapConf.formatOrInputAdapterName = ic.Format
      adapConf.adapterSpecificCfg = "{\"HostList\": \"%s\",\"TopicName\": \"%s\" }".format(ic.HostList, ic.Topic)
      idx += 1
      (adapConf, ic)
    })

    // Start all the neede Kafka Adapters for Output queues
    adapConfigs.foreach {
      case (conf, ic) => {
        // Create Kafka Consumer Here for the output queue
        val t_adapter = KafkaSimpleConsumer.CreateInputAdapter(conf, null, ExecContextObjImpl, SimpleStats)
        val t_adapterMeta: Array[(PartitionUniqueRecordKey, PartitionUniqueRecordValue)] = t_adapter.getAllPartitionBeginValues

        val topicVals = scala.collection.mutable.Map[Int, Long]()

        //  Initialize and start the adapter that is going to read the output queues here.
        val inputMeta = t_adapterMeta.map(partMeta => {
          val info = new StartProcPartInfo
          info._key = partMeta._1
          val off = getOffset(ic.PartitionStartOffsets, partMeta._1, partMeta._2)
          info._val = off
          info._validateInfoVal = off
          val k = info._key.asInstanceOf[KafkaPartitionUniqueRecordKey]
          val v = info._val.asInstanceOf[KafkaPartitionUniqueRecordValue]
          topicVals(k.PartitionId) = v.Offset;
          info
        }).toArray
        ProcessedOffsets(conf.Name) = topicVals
        NameToKafkaAdapterInfo(conf.Name) = ic

        t_adapter.StartProcessing(inputMeta, false)
        inputKafkaAdapters += t_adapter
      }
    }
  }

  private def Shutdown: Unit = {
    try {
      inputKafkaAdapters.foreach(ia => {
        ia.Shutdown
      })
      inputKafkaAdapters.clear
      val map = outMdlsInfoMap.toMap
      outMdlsInfoMap.clear
      map.foreach(f => {
        f._2.foreach(mdl => {
          if (mdl.os != null) {
            mdl.os.close
          }
        })
      })
    } catch {
      case e: Exception => {}
    }
  }

  def update(o: Observable, arg: AnyRef): Unit = {
    val sig = arg.toString
    LOG.debug("Received signal: " + sig)
    if (sig.compareToIgnoreCase("SIGTERM") == 0 || sig.compareToIgnoreCase("SIGINT") == 0 || sig.compareToIgnoreCase("SIGABRT") == 0) {
      LOG.warn("Got " + sig + " signal. Shutting down the process")
      Shutdown
      sys.exit(0)
    }
  }

  def processMdlResult(msg: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue): Unit = {
    import org.json4s.JsonDSL._
    try {
/*
      try {
        val k = uniqueKey.asInstanceOf[KafkaPartitionUniqueRecordKey]
        val v = uniqueVal.asInstanceOf[KafkaPartitionUniqueRecordValue]
        val tmpPartOffMap = ProcessedOffsets.getOrElse(k.Name, null)
        val partOffMap =
          if (tmpPartOffMap != null)
            tmpPartOffMap
          else
            scala.collection.mutable.Map[Int, Long]()

        ProcessedOffsets.synchronized {
          partOffMap(k.PartitionId) = v.Offset
          ProcessedOffsets(k.Name) = partOffMap
        }
      } catch {
        case e: Exception => {}
      }
*/
      implicit val jsonFormats = org.json4s.DefaultFormats
      val json = org.json4s.jackson.JsonMethods.parse(msg)
      if (json == null) {
        LOG.error("%s:Got Invalid JSON Message: %s ".format(Utils.GetCurDtTmStr, msg))
      }
      val map = json.values.asInstanceOf[Map[String, Any]]

      val mdlRes = map.getOrElse("ModelsResult", null)
      val sb = new StringBuilder()

      if (mdlRes != null) {
        val mdlsres = if (mdlRes.isInstanceOf[List[Any]]) mdlRes.asInstanceOf[List[Any]].toArray else if (mdlRes.isInstanceOf[Array[Any]]) mdlRes.asInstanceOf[Array[Any]] else Array[Any]()
        mdlsres.foreach(tmponemdlres => {
          val onemdlres = tmponemdlres.asInstanceOf[Map[String, Any]]
          val mdlNm = onemdlres.getOrElse("ModelName", null)
          val output = onemdlres.getOrElse("output", null)
          if (mdlNm != null && output != null) {
            val lowerMdlNm = mdlNm.toString.trim.toLowerCase
            val mdlInfo = outMdlsInfoMap.getOrElse(lowerMdlNm, null)
            if (mdlInfo != null) {
              val outarr = if (output.isInstanceOf[List[Any]]) output.asInstanceOf[List[Any]].toArray else if (output.isInstanceOf[Array[Any]]) output.asInstanceOf[Array[Any]] else Array[Any]()
              val outmap = outarr.map(a => {
                val amap = a.asInstanceOf[Map[String, Any]]
                (amap.getOrElse("Name", "").toString.toLowerCase, amap.getOrElse("Value", "").toString)
              }).toMap

              mdlInfo.foreach(oi => {
                sb.setLength(0)
                if (oi.cfg.Format.compareToIgnoreCase("delimited") == 0) {
                  var writtenBefore = false
                  oi.cfg.Fields.foreach(f => {
                    if (writtenBefore)
                      sb.append(oi.cfg.FieldDelimiter)
                    val v = outmap.getOrElse(f, "")
                    sb.append(v)
                    writtenBefore = true
                  })
                  sb.append(oi.cfg.RecordDelimiter)
                } else if (oi.cfg.Format.compareToIgnoreCase("json") == 0) {
                  if (oi.cfg.Fields.size > 0) { // Taking key & values, which are in fields
                    val json =
                      oi.cfg.Fields.toList.map(f =>
                        (("Name" -> f) ~
                          ("Value" -> outmap.getOrElse(f, ""))))
                    val o = compact(render(json))
                    sb.append(o)
                    sb.append(oi.cfg.RecordDelimiter)
                  } else { // Taking all key & values
                    val json =
                      outmap.toList.map(kv =>
                        (("Name" -> kv._1) ~
                          ("Value" -> kv._2)))
                    val o = compact(render(json))
                    sb.append(o)
                    sb.append(oi.cfg.RecordDelimiter)
                  }
                }
                if (sb.length > 0) {
                  oi.cfg.synchronized {
                    oi.os.write(sb.toString.getBytes("UTF8"))
                  }
                }
              })
            }
          }
        })
      }
    } catch {
      case e: Exception => {
        LOG.error("%s:Got Invalid JSON Message. Message:%s, Reason:%s, Message: %s ".format(Utils.GetCurDtTmStr, e.getMessage, e.getCause, msg))
        sys.exit(1)
      }
    }

  }

  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      PrintUsage()
      System.exit(1)
    }

    val options = nextOption(Map(), args.toList)
    val cfgfile = options.getOrElse('config, "").toString.trim
    if (cfgfile.size == 0) {
      LOG.error("%s:Configuration file missing".format(Utils.GetCurDtTmStr))
      sys.exit(1)
    }

    val tmpfl = new File(cfgfile)
    if (tmpfl.exists == false) {
      LOG.error("%s:Configuration file %s is not valid".format(Utils.GetCurDtTmStr, cfgfile))
      sys.exit(1)
    }

    var map = scala.collection.mutable.Map[String, Any]()
    try {
      val configJson = scala.io.Source.fromFile(cfgfile).mkString
      implicit val jsonFormats = org.json4s.DefaultFormats
      val json = org.json4s.jackson.JsonMethods.parse(configJson)
      if (json == null) {
        LOG.error("%s:Configuration file %s is not valid".format(Utils.GetCurDtTmStr, cfgfile))
        sys.exit(1)
      }
      val tmpmap = json.values.asInstanceOf[Map[String, Any]]

      // Convert all keys to lower case to access the value by name
      tmpmap.foreach(kv => {
        map(kv._1.toLowerCase) = kv._2
      })
    } catch {
      case e: Exception => {
        LOG.error("%s:Failed with exception. Message:%s, Reason:%s ".format(Utils.GetCurDtTmStr, e.getMessage, e.getCause))
        sys.exit(1)
      }
    }

    val input = map.getOrElse("input", null)
    val output = map.getOrElse("output", null)

    if (input == null) {
      LOG.error("%s:Not found Input in configuration file:%s".format(Utils.GetCurDtTmStr, cfgfile))
      sys.exit(1)
    }

    if (output == null) {
      LOG.error("%s:Not found Output in configuration file:%s".format(Utils.GetCurDtTmStr, cfgfile))
      sys.exit(1)
    }

    val inputConfig = ProcessInputConfig(input)
    val outputConfig = ProcessOutputConfig(output)

    ValidateInputConfig(inputConfig)
    ValidateOutputConfig(outputConfig)

    OpenOutputFiles(outputConfig)

    try {
      var sh: SignalHandler = null
      try {
        sh = new SignalHandler()
        sh.addObserver(this)
        sh.handleSignal("TERM")
        sh.handleSignal("INT")
        sh.handleSignal("ABRT")
      } catch {
        case e: Throwable => {
          LOG.error("Failed to add signal handler.\nStacktrace:" + StackTrace.ThrowableTraceString(e))
        }
      }
      /*
      scala.sys.addShutdownHook({
        LOG.warn("Got Shutdown request")
        Shutdown
        sys.exit(0)
      })
*/

      StartProcessingInput(inputConfig)
      println("Tool  is running now. Waiting for user to terminate with SIGTERM, SIGINT/Ctrl+C or SIGABRT signals")
      Thread.sleep(365L * 86400L * 1000L) // Waiting for a year
    } catch {
      case e: Exception => {
        LOG.error("%s:Exception:%s. Message:%s, Reason:%s".format(Utils.GetCurDtTmStr, e.toString, e.getMessage, e.getCause))
        Shutdown
      }
    }

    sys.exit(0)
  }
}

