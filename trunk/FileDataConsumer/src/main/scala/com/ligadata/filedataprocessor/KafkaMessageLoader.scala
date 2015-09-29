package com.ligadata.filedataprocessor

import java.io.{File, PrintWriter}
import java.util.Properties

import com.ligadata.Exceptions.{MsgCompilationFailedException, StackTrace}
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

  var delimiters = new DataDelimiters
  delimiters.keyAndValueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.KV_SEPARATOR,"\\x01")
  delimiters.fieldDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.FIELD_SEPARATOR,",")
  delimiters.valueDelimiter = inConfiguration.getOrElse(SmartFileAdapterConstants.VALUE_SEPARATOR,"~")

  MetadataAPIImpl.InitMdMgrFromBootStrap(inConfiguration(SmartFileAdapterConstants.METADATA_CONFIG_FILE), false)

  val loaderInfo = new KamanjaLoaderInfo()
  println("Getting "+ inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME))

  var msgDefName = inConfiguration(SmartFileAdapterConstants.MESSAGE_NAME).split('.')
  var msgDef: MessageDef = mdMgr.ActiveMessage(msgDefName(0), msgDefName(1))

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
      println("\n\n" + objInst.asInstanceOf[MessageContainerObjBase].FullName + "\n\n" + objInst.asInstanceOf[MessageContainerObjBase].Version)
    }
  })

  // Ok, get the MessageBaseObj for messagedef..

  // create the producer object
  val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))


  def pushData(messages: Array[KafkaMessage]): Unit = {

    messages.foreach(msg => {

      println("\nKafkaMessage:\n  File: " + msg.relatedFileName+", offset:  "+ msg.offsetInFile + "\n " + new String(msg.msg))
      var inputData =  CreateKafkaInput(new String(msg.msg), SmartFileAdapterConstants.MESSAGE_NAME, delimiters)
      println(inputData.asInstanceOf[KvData].dataMap.foreach(x => {println(x._1 + "<>" +x._2)}))  //mkString("*"))
      println(" PartitionKey is " + objInst.asInstanceOf[MessageContainerObjBase].PartitionKeyData(inputData).mkString("--"))

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
    })
  //  pw.close
  }

  /**
   * send message
   */
  private def send(producer: Producer[AnyRef, AnyRef], topic: String, message: Array[Byte], partIdx: Array[Byte]): Boolean = {
    try {
      producer.send(new KeyedMessage(topic, partIdx, message))
      return true
    } catch {
      case e: Exception =>
        e.printStackTrace()
        return false
    }
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

    println(inputData)

    val str_arr = inputData.split(delimiters.fieldDelimiter, -1)
    val inpData = new KvData(inputData, delimiters)
    val dataMap = scala.collection.mutable.Map[String, String]()

    println("1 ."+delimiters.fieldDelimiter+"."+delimiters.keyAndValueDelimiter+"."+delimiters.valueDelimiter+".")

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
