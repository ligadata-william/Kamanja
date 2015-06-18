
package com.ligadata.FatafatManager

import com.ligadata.FatafatBase.{ EnvContext, ExecContext, InputAdapter, OutputAdapter, MakeExecContext, PartitionUniqueRecordKey, PartitionUniqueRecordValue }

import org.apache.log4j.Logger
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.ArrayBuffer

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ExecContextImpl(val input: InputAdapter, val curPartitionId: Int, val output: Array[OutputAdapter], val envCtxt: EnvContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionId, output)
  def execute(transId: Long, data: String, format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, processingXformMsg: Int, totalXformMsg: Int, associatedMsg: String, delimiterString: String): Unit = {
    try {
      val uk = uniqueKey.Serialize
      val uv = uniqueVal.Serialize

      try {
        val transformStartTime = System.nanoTime
        val xformedmsgs = xform.execute(data, format, associatedMsg, delimiterString)
        LOG.debug(ManagerUtils.getComponentElapsedTimeStr("Transform", uv, readTmNanoSecs, transformStartTime))
        var xformedMsgCntr = 0
        val totalXformedMsgs = xformedmsgs.size
        xformedmsgs.foreach(xformed => {
          xformedMsgCntr += 1
          envCtxt.setAdapterUniqueKeyValue(transId, uk, uv, xformedMsgCntr, totalXformedMsgs)
          engine.execute(transId, xformed._1, xformed._2, xformed._3, envCtxt, readTmNanoSecs, readTmMilliSecs, uk, uv, xformedMsgCntr, totalXformedMsgs, (ignoreOutput && xformedMsgCntr <= processingXformMsg))
        })
      } catch {
        case e: Exception => {
          LOG.error("Failed to execute message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
          e.printStackTrace()
        }
      } finally {
        // LOG.debug("UniqueKeyValue:%s => %s".format(uk, uv))
        val commitStartTime = System.nanoTime
        envCtxt.commitData(transId)
        LOG.debug(ManagerUtils.getComponentElapsedTimeStr("Commit", uv, readTmNanoSecs, commitStartTime))
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to serialize uniqueKey/uniqueVal. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
}

object MakeExecContextImpl extends MakeExecContext {
  def CreateExecContext(input: InputAdapter, curPartitionId: Int, output: Array[OutputAdapter], envCtxt: EnvContext): ExecContext = {
    new ExecContextImpl(input, curPartitionId, output, envCtxt)
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
class ValidateExecCtxtImpl(val input: InputAdapter, val curPartitionId: Int, val output: Array[OutputAdapter], val envCtxt: EnvContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionId, output)

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

  def execute(transId: Long, data: String, format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean, processingXformMsg: Int, totalXformMsg: Int, associatedMsg: String, delimiterString: String): Unit = {
    try {
      try {
        val json = parse(data)
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
        envCtxt.commitData(transId)
      }
    } catch {
      case e: Exception => {
        LOG.error("Failed to serialize uniqueKey/uniqueVal. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
}

object MakeValidateExecCtxtImpl extends MakeExecContext {
  def CreateExecContext(input: InputAdapter, curPartitionId: Int, output: Array[OutputAdapter], envCtxt: EnvContext): ExecContext = {
    new ValidateExecCtxtImpl(input, curPartitionId, output, envCtxt)
  }
}

