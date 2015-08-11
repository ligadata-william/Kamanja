
package com.ligadata.KamanjaManager

import com.ligadata.KamanjaBase.{ EnvContext }
import com.ligadata.InputOutputAdapterInfo.{ ExecContext, InputAdapter, OutputAdapter, ExecContextObj, PartitionUniqueRecordKey, PartitionUniqueRecordValue, InputAdapterCallerContext }

import org.apache.log4j.Logger
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer

import com.ligadata.transactions._

import com.ligadata.transactions._

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ExecContextImpl(val input: InputAdapter, val curPartitionKey: PartitionUniqueRecordKey, val callerCtxt: InputAdapterCallerContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);
  if (callerCtxt.isInstanceOf[KamanjaInputAdapterCallerContext] == false) {
    throw new Exception("Handling only KamanjaInputAdapterCallerContext in ValidateExecCtxtImpl")
  }
  
  NodeLevelTransService.init(KamanjaConfiguration.zkConnectString, KamanjaConfiguration.zkSessionTimeoutMs, KamanjaConfiguration.zkConnectionTimeoutMs, KamanjaConfiguration.zkNodeBasePath, KamanjaConfiguration.txnIdsRangeForNode, KamanjaConfiguration.dataDataStoreInfo, KamanjaConfiguration.jarPaths)

  val kamanjaCallerCtxt = callerCtxt.asInstanceOf[KamanjaInputAdapterCallerContext]
  val transService = new SimpleTransService
  transService.init(KamanjaConfiguration.txnIdsRangeForPartition)

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionKey, kamanjaCallerCtxt.outputAdapters)
  def execute(data: Array[Byte], format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, processingXformMsg: Int, totalXformMsg: Int, associatedMsg: String, delimiterString: String): Unit = {
    try {
      val uk = uniqueKey.Serialize
      val uv = uniqueVal.Serialize
      val transId = transService.getNextTransId
      LOG.debug("Processing uniqueKey:%s, uniqueVal:%s, Datasize:%d".format(uk, uv, data.size))

      try {
        val transformStartTime = System.nanoTime
        val xformedmsgs = xform.execute(data, format, associatedMsg, delimiterString, uk, uv)
        LOG.info(ManagerUtils.getComponentElapsedTimeStr("Transform", uv, readTmNanoSecs, transformStartTime))
        var xformedMsgCntr = 0
        val totalXformedMsgs = xformedmsgs.size
        xformedmsgs.foreach(xformed => {
          xformedMsgCntr += 1
          kamanjaCallerCtxt.envCtxt.setAdapterUniqueKeyValue(transId, uk, uv, xformedMsgCntr, totalXformedMsgs)
          engine.execute(transId, xformed._1, xformed._2, xformed._3, kamanjaCallerCtxt.envCtxt, readTmNanoSecs, readTmMilliSecs, uk, uv, xformedMsgCntr, totalXformedMsgs, (ignoreOutput && xformedMsgCntr <= processingXformMsg))
        })
      } catch {
        case e: Exception => {
          LOG.error("Failed to execute message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          e.printStackTrace()
        }
      } finally {
        // LOG.debug("UniqueKeyValue:%s => %s".format(uk, uv))
        val commitStartTime = System.nanoTime
        val containerData = kamanjaCallerCtxt.envCtxt.getChangedData(transId, false, true) // scala.collection.immutable.Map[String, List[List[String]]]
        kamanjaCallerCtxt.envCtxt.commitData(transId)
        if (containerData != null && containerData.size > 0) {
          val datachangedata = ("txnid" -> transId.toString) ~
            ("changeddatakeys" -> containerData.map(kv =>
              ("C" -> kv._1) ~
                ("K" -> kv._2)))
          val sendJson = compact(render(datachangedata))
          KamanjaLeader.SetNewDataToZkc(KamanjaConfiguration.zkNodeBasePath + "/datachange", sendJson.getBytes("UTF8"))
        }
        LOG.info(ManagerUtils.getComponentElapsedTimeStr("Commit", uv, readTmNanoSecs, commitStartTime))
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to serialize uniqueKey/uniqueVal. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
}

object ExecContextObjImpl extends ExecContextObj {
  def CreateExecContext(input: InputAdapter, curPartitionKey: PartitionUniqueRecordKey, callerCtxt: InputAdapterCallerContext): ExecContext = {
    new ExecContextImpl(input, curPartitionKey, callerCtxt)
  }
}

//--------------------------------

object CollectKeyValsFromValidation {
  // Key to (Value, xCntr, xTotl & TxnId)
  private[this] val keyVals = scala.collection.mutable.Map[String, (String, Int, Int, Long)]()
  private[this] val lock = new Object()
  private[this] var lastUpdateTime = System.nanoTime

  def addKeyVals(uKStr: String, uVStr: String, xCntr: Int, xTotl: Int, txnId: Long): Unit = lock.synchronized {
    val klc = uKStr.toLowerCase
    val existVal = keyVals.getOrElse(klc, null)
    if (existVal == null || txnId > existVal._4) {
      keyVals(klc) = (uVStr, xCntr, xTotl, txnId)
    }
    lastUpdateTime = System.nanoTime
  }

  def get: scala.collection.immutable.Map[String, (String, Int, Int, Long)] = lock.synchronized {
    keyVals.toMap
  }

  def clear: Unit = lock.synchronized {
    keyVals.clear
    lastUpdateTime = System.nanoTime
  }

  def getLastUpdateTime: Long = lock.synchronized {
    lastUpdateTime
  }
}

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ValidateExecCtxtImpl(val input: InputAdapter, val curPartitionKey: PartitionUniqueRecordKey, val callerCtxt: InputAdapterCallerContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);

  if (callerCtxt.isInstanceOf[KamanjaInputAdapterCallerContext] == false) {
    throw new Exception("Handling only KamanjaInputAdapterCallerContext in ValidateExecCtxtImpl")
  }

  val kamanjaCallerCtxt = callerCtxt.asInstanceOf[KamanjaInputAdapterCallerContext]

  
  NodeLevelTransService.init(KamanjaConfiguration.zkConnectString, KamanjaConfiguration.zkSessionTimeoutMs, KamanjaConfiguration.zkConnectionTimeoutMs, KamanjaConfiguration.zkNodeBasePath, KamanjaConfiguration.txnIdsRangeForNode, KamanjaConfiguration.dataDataStoreInfo, KamanjaConfiguration.jarPaths)

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionKey, kamanjaCallerCtxt.outputAdapters)
  val transService = new SimpleTransService

  transService.init(KamanjaConfiguration.txnIdsRangeForPartition)
  
  private def getAllModelResults(data: Any): Array[Map[String, Any]] = {
    val results = new ArrayBuffer[Map[String, Any]]()
    try {
      data match {
        case m: Map[_, _] => {
          try {
            results += m.asInstanceOf[Map[String, Any]]
          } catch {
            case e: Exception => {
              LOG.error("Failed reason %s, message %s".format(e.getCause, e.getMessage))
            }
          }
        }
        case l: List[Any] => {
          try {
            val data = l.asInstanceOf[List[Any]]
            data.foreach(d => {
              results ++= getAllModelResults(d)
            })
          } catch {
            case e: Exception => {
              LOG.error("Failed reason %s, message %s".format(e.getCause, e.getMessage))
            }
          }
        }
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to collect model results. Reason %s, message %s".format(e.getCause, e.getMessage))
      }
    }

    results.toArray
  }

  def execute(data: Array[Byte], format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, processingXformMsg: Int, totalXformMsg: Int, associatedMsg: String, delimiterString: String): Unit = {
    try {
      val transId = transService.getNextTransId
      try {
        val json = parse(new String(data))
        if (json == null || json.values == null) {
          LOG.error("Invalid JSON data : " + data)
          return
        }
        val parsed_json = json.values.asInstanceOf[Map[String, Any]]
        if (parsed_json.size != 1) {
          LOG.error("Expecting only one ModelsResult in JSON data : " + data)
          return
        }
        val mdlRes = parsed_json.head._1
        if (mdlRes == null || mdlRes.compareTo("ModelsResult") != 0) {
          LOG.error("Expecting only ModelsResult as key in JSON data : " + data)
          return
        }

        val results = getAllModelResults(parsed_json.head._2)

        results.foreach(allVals => {
          val uK = allVals.getOrElse("uniqKey", null)
          val uV = allVals.getOrElse("uniqVal", null)

          if (uK == null || uV == null) {
            LOG.error("Not found uniqKey & uniqVal in ModelsResult. JSON string : " + data)
            return
          }

          val uKStr = uK.toString
          val uVStr = uV.toString
          val xCntr = allVals.getOrElse("xformCntr", "1").toString.toInt
          val xTotl = allVals.getOrElse("xformTotl", "1").toString.toInt
          val txnId = allVals.getOrElse("TxnId", "0").toString.toLong
          val existVal = CollectKeyValsFromValidation.addKeyVals(uKStr, uVStr, xCntr, xTotl, txnId)
        })
      } catch {
        case e: Exception => {
          LOG.error("Failed to execute message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        }
      } finally {
        // LOG.debug("UniqueKeyValue:%s => %s".format(uk, uv))
        kamanjaCallerCtxt.envCtxt.commitData(transId)
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to serialize uniqueKey/uniqueVal. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
}

object ValidateExecContextObjImpl extends ExecContextObj {
  def CreateExecContext(input: InputAdapter, curPartitionKey: PartitionUniqueRecordKey, callerCtxt: InputAdapterCallerContext): ExecContext = {
    new ValidateExecCtxtImpl(input, curPartitionKey, callerCtxt)
  }
}

