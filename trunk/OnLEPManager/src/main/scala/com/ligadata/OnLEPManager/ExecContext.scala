
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.{ EnvContext, ExecContext, InputAdapter, OutputAdapter, MakeExecContext, PartitionUniqueRecordKey, PartitionUniqueRecordValue }

import org.apache.log4j.Logger

// There are no locks at this moment. Make sure we don't call this with multiple threads for same object
class ExecContextImpl(val input: InputAdapter, val curPartitionId: Int, val output: Array[OutputAdapter], val envCtxt: EnvContext) extends ExecContext {
  val LOG = Logger.getLogger(getClass);

  val xform = new TransformMessageData
  val engine = new LearningEngine(input, curPartitionId, output)
  def execute(data: String, uniqueKey: PartitionUniqueRecordKey, uniqueVal: PartitionUniqueRecordValue, readTmNanoSecs: Long, readTmMilliSecs: Long): Unit = {
    try {
      val xformed = xform.execute(data)
      engine.execute(xformed._2, xformed._1, xformed._3, envCtxt, readTmNanoSecs, readTmMilliSecs)
    } catch {
      case e: Exception => {
        LOG.error("Failed to execute message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
      }
    }
  }
}

object MakeExecContextImpl extends MakeExecContext {
  def CreateExecContext(input: InputAdapter, curPartitionId: Int, output: Array[OutputAdapter], envCtxt: EnvContext): ExecContext = {
    new ExecContextImpl(input, curPartitionId, output, envCtxt)
  }
}
