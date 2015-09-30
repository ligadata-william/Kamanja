package com.ligadata.filedataprocessor

import java.io.{IOException, File, PrintWriter}
import java.nio.file.StandardCopyOption._
import java.nio.file.{Paths, Files}
import java.util.Properties

import com.ligadata.Exceptions.{UnsupportedObjectException, MsgCompilationFailedException, StackTrace}
import com.ligadata.KamanjaBase._
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Utils.{Utils, KamanjaLoaderInfo}
import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata.MessageDef
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by danielkozin on 9/24/15.
 */
class KafkaMessageLoader(inConfiguration: scala.collection.mutable.Map[String, String]) {
  val pw = new PrintWriter(new File("/tmp/output.txt" ))
  var partIdx: Int = 0
  var lastFileProcessing: String = ""
  var lastOffsetProcessed: Int = 0

  var objInst: Any = null
  // Set up some properties for the Kafka Producer
  val props = new Properties()
  props.put("metadata.broker.list", inConfiguration(SmartFileAdapterConstants.KAFKA_BROKER));
  props.put("request.required.acks", "1")
  // create the producer object
  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))

  var delimiters = new DataDelimiters
  delimiters.keyAndValueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.KV_SEPARATOR,"\\x01")
  delimiters.fieldDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.FIELD_SEPARATOR,"\\x01")
  delimiters.valueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.VALUE_SEPARATOR,"~")

  MetadataAPIImpl.InitMdMgrFromBootStrap(inConfiguration(SmartFileAdapterConstants.METADATA_CONFIG_FILE), false)

  val loaderInfo = new KamanjaLoaderInfo()
  println("Getting "+ inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))

  //var msgDefName = inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME).split('.')
  val(typNameSpace, typName) = com.ligadata.kamanja.metadata.Utils.parseNameTokenNoVersion(inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))
  println(typNameSpace)
  println(typName)
  var msgDef: MessageDef = mdMgr.ActiveMessage(typNameSpace, typName)

  if (msgDef == null) {
    throw new UnsupportedObjectException("Unknown message definition")
  }
  // Just in case we want this to deal with more then 1 MSG_DEF in a future.  - msgName paramter will probably have to
  // be an array inthat case.. but for now......
  var allJars = collection.mutable.Set[String]()
  allJars = allJars + msgDef.jarName

  var jarPaths0 = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
  jarPaths0 = jarPaths0 + MetadataAPIImpl.GetMetadataAPIConfig.getProperty("COMPILER_WORK_DIR")

  Utils.LoadJars(allJars.map(j => Utils.GetValidJarFile(jarPaths0, j)).toArray, loaderInfo.loadedJars, loaderInfo.loader)
  val jarName0 = Utils.GetValidJarFile(jarPaths0, msgDef.jarName)
  var classNames = Utils.getClasseNamesInJar(jarName0)


  var tempCurClass: Class[_] = null
  classNames.foreach(clsName => {
    try {
      Class.forName(clsName, true, loaderInfo.loader)
    } catch {
      case e: Exception => {
        logger.error("Failed to load Model class %s with Reason:%s Message:%s".format(clsName, e.getCause, e.getMessage))
        throw e // Rethrow
      }
    }

    var curClz = Class.forName(clsName, true, loaderInfo.loader)
    tempCurClass = curClz

    var isMsg = false
    while (curClz != null && isMsg == false) {
      isMsg = Utils.isDerivedFrom(curClz, "com.ligadata.KamanjaBase.BaseMsgObj")
      if (isMsg == false)
        curClz = curClz.getSuperclass()
    }

    if (isMsg) {
      try {
        // Trying Singleton Object
        val module = loaderInfo.mirror.staticModule(clsName)
        val obj = loaderInfo.mirror.reflectModule(module)
        objInst = obj.instance
      } catch {
        case e: java.lang.NoClassDefFoundError => {
          e.printStackTrace()
          throw e
        }
        case e: Exception => {
          objInst = tempCurClass.newInstance
        }
      }
    }
  })


  def pushData(messages: Array[KafkaMessage]): Unit = {

    messages.foreach(msg => {
      // Now, there are some special cases here.  If offset is -1, then its just a signal to close the file
      // else, this may or may not be the last message in the file... look to isLast for guidance.
      if(msg.offsetInFile > 0) {
        println("\nKafkaMessage:\n  File: " + msg.relatedFileName+", offset:  "+ msg.offsetInFile + "\n " + new String(msg.msg))
        var inputData =  CreateKafkaInput(new String(msg.msg), SmartFileAdapterConstants.MESSAGE_NAME, delimiters)

        try {
          producer.send(new KeyedMessage(inConfiguration(SmartFileAdapterConstants.KAFKA_TOPIC),
            objInst.asInstanceOf[MessageContainerObjBase].PartitionKeyData(inputData).mkString.getBytes("UTF8"),
            new String(msg.msg).getBytes("UTF8")))

          lastFileProcessing = msg.relatedFileName
          lastOffsetProcessed = msg.offsetInFile
          println("Message added")
        } catch {
          case e: Exception =>
            logger.error("Could not add to the queue due to an Exception "+ e.getMessage)
            e.printStackTrace
        }
      }

      if (msg.isLast) {
        try {
          println("EOF Detected")
          var fileStruct = msg.relatedFileName.split("/")
          if (inConfiguration.getOrElse(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO, null) != null) {
            println(" Moving File" + msg.relatedFileName + " to "+ inConfiguration(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO))
            Files.copy(Paths.get(msg.relatedFileName), Paths.get(inConfiguration(SmartFileAdapterConstants.DIRECTORY_TO_MOVE_TO)+"/"+ fileStruct(fileStruct.size - 1)),REPLACE_EXISTING)
            Files.deleteIfExists(Paths.get(msg.relatedFileName))
          } else {
            println(" Renaming file "+ msg.relatedFileName + " to " + msg.relatedFileName + "_COMPLETE")
            (new File(msg.relatedFileName)).renameTo(new File(msg.relatedFileName + "_COMPLETE"))
          }
        } catch {
          case ioe: IOException => ioe.printStackTrace()
        }
      }

    })
  }



  /**
   *
   * @param inputData
   * @param associatedMsg
   * @param delimiters
   * @return
   */
  private def CreateKafkaInput(inputData: String, associatedMsg: String, delimiters: DataDelimiters): InputData = {
    if (associatedMsg == null || associatedMsg.size == 0)
      throw new Exception("KV data expecting Associated messages as input.")

    if (delimiters.fieldDelimiter == null) delimiters.fieldDelimiter = ","
    if (delimiters.valueDelimiter == null) delimiters.valueDelimiter = "~"
    if (delimiters.keyAndValueDelimiter == null) delimiters.keyAndValueDelimiter = "\\x01"

    val str_arr = inputData.split(delimiters.fieldDelimiter, -1)
    val inpData = new KvData(inputData, delimiters)
    val dataMap = scala.collection.mutable.Map[String, String]()

    if (delimiters.fieldDelimiter.compareTo(delimiters.keyAndValueDelimiter) == 0) {
      if (str_arr.size % 2 != 0) {
        val errStr = "Expecting Key & Value pairs are even number of tokens when FieldDelimiter & KeyAndValueDelimiter are matched. We got %d tokens from input string %s".format(str_arr.size, inputData)
        println(errStr)
        throw new Exception(errStr)
      }
      for (i <- 0 until str_arr.size by 2) {
        dataMap(str_arr(i).trim) = str_arr(i + 1)
      }
    } else {
      str_arr.foreach(kv => {
        val kvpair = kv.split(delimiters.keyAndValueDelimiter)
        if (kvpair.size != 2) {
          throw new Exception("Expecting Key & Value pair only")
        }
        dataMap(kvpair(0).trim) = kvpair(1)
      })
    }

    inpData.dataMap = dataMap.toMap
    inpData

  }
}
