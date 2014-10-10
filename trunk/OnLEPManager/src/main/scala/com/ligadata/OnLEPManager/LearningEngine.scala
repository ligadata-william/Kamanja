
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.{ BaseMsg, DelimitedData, JsonData, XmlData, EnvContext }
import com.ligadata.Utils.Utils
import java.util.Map
import scala.util.Random
import com.ligadata.OnLEPBase.{ MdlInfo, BaseMsgObj, BaseMsg, InputAdapter, OutputAdapter }
import org.apache.log4j.Logger
import java.io.{ PrintWriter, File }

class LearningEngine(val input: InputAdapter, val processingPartitionId: Int, val output: Array[OutputAdapter]) {
  val LOG = Logger.getLogger(getClass);
  var cntr: Long = 0
  var totalLatencyFromReadToProcess: Long = 0
  // var totalLatencyFromReadToOutput: Long = 0

  val rand = new Random(hashCode)

  private def createMsg(msgType: String, msgFormat: String, msgData: String): BaseMsg = {
    val msgInfo = OnLEPMetadata.getMessgeInfo(msgType)
    if (msgInfo != null) {
      val msg: BaseMsg = msgInfo.msgobj.CreateNewMessage
      if (msgFormat.equalsIgnoreCase("csv")) {
        msg.populate(new DelimitedData(msgData, ","))
      } else if (msgFormat.equalsIgnoreCase("json")) {
        msg.populate(new JsonData(msgData))
      } else if (msgFormat.equalsIgnoreCase("xml")) {
        msg.populate(new XmlData(msgData))
      } else {
        throw new Exception("Invalid input data type:" + msgFormat)
        return null
      }
      msg
    } else {
      throw new Exception("Not found Message Type:" + msgType)
      null
    }
  }

  private def RunAllModels(msg: BaseMsg, msgData: String, envContext: EnvContext, readTmNs: Long, rdTmMs: Long): Unit = {
    if (msg == null)
      return

    val models: Array[MdlInfo] = OnLEPMetadata.modelObjects.map(mdl => mdl._2).toArray
    var result: StringBuilder = new StringBuilder(8 * 1024)

    result ++= "{\"ModelsResult\" : ["

    // var executedMdls = 0
    var gotResults = 0

    val outputAlways: Boolean = false; // (rand.nextInt(9) == 5) // For now outputting ~(1 out of 9) randomly when we get random == 5

    // Execute all modes here
    models.foreach(md => {
      try {

        if (md.mdl.IsValidMessage(msg)) { // Checking whether this message has any fields/concepts to execute in this model
          // LOG.info("Found Valid Message:" + msgData)
          // executedMdls += 1
          val curMd = md.mdl.CreateNewModel(envContext, msg, md.tenantId)
          if (curMd != null) {
            val res = curMd.execute(outputAlways)
            if (res != null) {
              if (gotResults > 0)
                result ++= ","
              result ++= res.toJsonString(readTmNs, rdTmMs)
              gotResults = gotResults + 1
            } else {
              // Nothing to output
            }
          } else {
            LOG.error("Failed to create model " + md.mdl.getModelName)
          }
        } else {
          // LOG.info("Found Invalid Message:" + msgData)
        }
      } catch {
        case e: Exception => { LOG.error("Model Failed => " + md.mdl.getModelName + ". Error: " + e.getMessage /* + "\n Trace:\n" + e.printStackTrace() */ ) }
      }
    })

    result ++= "]}"

    if (gotResults > 0 && output != null) {
      val resStr = result.toString
      output.foreach(o => {
        o.send(resStr, cntr.toString)
      })
    }
  }

  def execute(msgType: String, msgFormat: String, msgData: String, envContext: EnvContext, readTmNs: Long, rdTmMs: Long): Unit = {
    // LOG.info("LE => " + msgData)
    try {
      // BUGBUG:: for now handling only CSV input data.
      val msg = createMsg(msgType, msgFormat, msgData)
      if (msg != null) {
        // Run all models
        // BUGBUG::Get Previous History (through Key) of the top level message/container 
        RunAllModels(msg, msgData, envContext, readTmNs, rdTmMs)
        var latencyFromReadToProcess = (System.nanoTime - readTmNs) / 1000 // Nanos to micros
        if (latencyFromReadToProcess < 0) latencyFromReadToProcess = 40 // taking minimum 40 micro secs
        totalLatencyFromReadToProcess += latencyFromReadToProcess
        //BUGBUG:: Save the whole message here
      }
    } catch {
      case e: Exception => LOG.error("Failed to create and run message. Error:" + e.getMessage)
    }

    cntr += 1
  }
}

