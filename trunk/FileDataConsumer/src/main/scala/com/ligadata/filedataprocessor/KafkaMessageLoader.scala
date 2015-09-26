package com.ligadata.filedataprocessor

import java.io.{File, PrintWriter}
import java.util.Properties

import com.ligadata.Exceptions.{MsgCompilationFailedException, StackTrace}
import com.ligadata.KamanjaBase.{DelimitedData, MessageContainerObjBase}
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Utils.{Utils, KamanjaLoaderInfo}
import com.ligadata.kamanja.metadata.MdMgr._
import com.ligadata.kamanja.metadata.MessageDef
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by danielkozin on 9/24/15.
 */
class KafkaMessageLoader(broker: String, topic: String, mdConfig: String, msgName: String) {
  val pw = new PrintWriter(new File("/tmp/output.txt" ))
  var partIdx: Int = 0

  var objInst: Any = null
  // Set up some properties for the Kafka Producer
  val props = new Properties()
  props.put("metadata.broker.list", broker);
  props.put("request.required.acks", "1")
  MetadataAPIImpl.InitMdMgrFromBootStrap(mdConfig, false)

  val loaderInfo = new KamanjaLoaderInfo()
  println("Getting "+ msgName)
  var msgDefName = msgName.split('.')
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

  def pushData(messages: Array[Array[Byte]]): Unit = {
    //
  }

  def pushData(messages: Array[KafkaMessage]): Unit = {

    messages.foreach(msg => {
   //   pw.write(msg.msg)
  //    pw.write('\n')
      println("\nKafkaMessage:\n  File: " + msg.relatedFileName+", offset:  "+ msg.offsetInFile + "\n " + new String(msg.msg))
      var inputData = new DelimitedData(new String(msg.msg), ",")
      inputData.tokens = new String(msg.msg).split(",")
      println(inputData.dataInput)  //mkString("*"))
      println(" PartitionKey is " + objInst.asInstanceOf[MessageContainerObjBase].PartitionKeyData(inputData).mkString("--"))
    })
  //  pw.close
  }

  // Push the data into kafka
  private def insertData(msg: String): Unit = {

    if (send(producer, topic, msg.getBytes("UTF8"), partIdx.toString.getBytes("UTF8"))) {
      partIdx = partIdx + 1
      //return "SUCCESS: Added message to Topic:"+topic
    } else {
      println("FAILURE: Failed to add message to Topic:"+topic)
    }
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
}
