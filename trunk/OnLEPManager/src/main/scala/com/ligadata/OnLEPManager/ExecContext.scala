
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.{ EnvContext, ExecContext, InputAdapter, OutputAdapter, MakeExecContext, PartitionUniqueRecordKey, PartitionUniqueRecordValue }

import org.apache.log4j.Logger

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ExecContextImpl(val input: InputAdapter, val curPartitionId: Int, val output: Array[OutputAdapter], val envCtxt: EnvContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionId, output)
  def execute(tempTransId: Long, data: String, format: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long, ignoreOutput: Boolean): Unit = {

    try {
      val uk = uniqueKey.Serialize
      val uv = uniqueVal.Serialize
      try {
        val xformedmsgs = xform.execute(data, format)
        var xformedMsgCntr = 0
        val totalXformedMsgs = xformedmsgs.size
        xformedmsgs.foreach(xformed => {
          engine.execute(tempTransId, xformed._2, xformed._1, xformed._3, envCtxt, readTmNanoSecs, readTmMilliSecs, uk, uv, xformedMsgCntr, totalXformedMsgs, ignoreOutput)
          xformedMsgCntr += 1
        })
      } catch {
        case e: Exception => {
          LOG.error("Failed to execute message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
        }
      } finally {
        // LOG.info("UniqueKeyValue:%s => %s".format(uk, uv))
        envCtxt.setAdapterUniqueKeyValue(tempTransId, uk, uv)
        envCtxt.commitData(tempTransId)
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
