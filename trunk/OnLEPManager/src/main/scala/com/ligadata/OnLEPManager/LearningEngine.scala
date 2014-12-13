
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.{ BaseMsg, DelimitedData, JsonData, XmlData, EnvContext, ModelResult }
import com.ligadata.Utils.Utils
import java.util.Map
import scala.util.Random
import com.ligadata.OnLEPBase.{ MdlInfo, MessageContainerBase, BaseMsgObj, BaseMsg, BaseContainer, InputAdapter, OutputAdapter }
import org.apache.log4j.Logger
import java.io.{ PrintWriter, File }
import scala.xml.XML
import scala.xml.Elem
// import scala.util.parsing.json.JSON
import scala.collection.mutable.ArrayBuffer
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

class LearningEngine(val input: InputAdapter, val processingPartitionId: Int, val output: Array[OutputAdapter]) {
  val LOG = Logger.getLogger(getClass);
  var cntr: Long = 0
  var totalLatencyFromReadToProcess: Long = 0
  // var totalLatencyFromReadToOutput: Long = 0

  val rand = new Random(hashCode)

  private def createMsg(msgType: String, msgFormat: String, msgData: String): BaseMsg = {
    val msgInfo = OnLEPMetadata.getMessgeInfo(msgType)
    if (msgInfo != null) {
      val msg: BaseMsg = msgInfo.msgobj.asInstanceOf[BaseMsgObj].CreateNewMessage
      if (msgFormat.equalsIgnoreCase("csv")) {
        try {
          val inputData = new DelimitedData(msgData, ",")
          inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
          inputData.curPos = 0
          msg.populate(inputData)
        } catch {
          case e: Exception => {
            LOG.error("Failed to populate CSV data for messageType:%s, Reason:%s, ErrorMessage:%s".format(msgType, e.getCause, e.getMessage))
            throw e
          }
        }
      } else if (msgFormat.equalsIgnoreCase("json")) {
        try {
          val inputData = new JsonData(msgData)
          val json = parse(inputData.dataInput)
          val parsed_json = json.values.asInstanceOf[scala.collection.immutable.Map[String, Any]]
          if (parsed_json.size != 1)
            throw new Exception("Expecting only one message in JSON data : " + msgData)
          inputData.root_json = Option(parsed_json)
          inputData.cur_json = Option(parsed_json.head._2)
          msg.populate(inputData)
        } catch {
          case e: Exception => {
            LOG.error("Failed to populate JSON data for messageType:%s, Reason:%s, ErrorMessage:%s".format(msgType, e.getCause, e.getMessage))
            throw e
          }
        }
      } else if (msgFormat.equalsIgnoreCase("xml")) {
        try {
          val inputData = new XmlData(msgData)
          inputData.root_xml = XML.loadString(inputData.dataInput)
          inputData.cur_xml = inputData.root_xml
          msg.populate(inputData)
        } catch {
          case e: Exception => {
            LOG.error("Failed to populate XML data for messageType:%s, Reason:%s, ErrorMessage:%s".format(msgType, e.getCause, e.getMessage))
            throw e
          }
        }
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

  private def RunAllModels(tempTransId: Long, finalTopMsgOrContainer: MessageContainerBase, envContext: EnvContext): Array[ModelResult] = {
    var results: ArrayBuffer[ModelResult] = new ArrayBuffer[ModelResult]()

    if (finalTopMsgOrContainer != null) {

      val models: Array[MdlInfo] = OnLEPMetadata.getAllModels.map(mdl => mdl._2).toArray

      val outputAlways: Boolean = false; // (rand.nextInt(9) == 5) // For now outputting ~(1 out of 9) randomly when we get random == 5

      // Execute all modes here
      models.foreach(md => {
        try {

          if (md.mdl.IsValidMessage(finalTopMsgOrContainer)) { // Checking whether this message has any fields/concepts to execute in this model
            val curMd = md.mdl.CreateNewModel(tempTransId, envContext, finalTopMsgOrContainer, md.tenantId)
            if (curMd != null) {
              val res = curMd.execute(outputAlways)
              if (res != null) {
                results += res
              } else {
                // Nothing to output
              }
            } else {
              LOG.error("Failed to create model " + md.mdl.getModelName)
            }
          } else {
          }
        } catch {
          case e: Exception => { LOG.error("Model Failed => " + md.mdl.getModelName + ". Reason: " + e.getCause + ". Message: " + e.getMessage + "\n Trace:\n" + e.printStackTrace()) }
        }
      })
    }
    return results.toArray
  }

  private def GetTopMsgName(msgName: String): (String, Boolean, MsgContainerObjAndTransformInfo) = {
    val topMsgInfo = OnLEPMetadata.getMessgeInfo(msgName)
    if (topMsgInfo == null || topMsgInfo.parents.size == 0) return (msgName, false, null)
    (topMsgInfo.parents(0)._1, true, topMsgInfo)
  }

  def execute(tempTransId: Long, msgType: String, msgFormat: String, msgData: String, envContext: EnvContext, readTmNs: Long, rdTmMs: Long, uk: String, uv: String, xformedMsgCntr: Int, totalXformedMsgs: Int, ignoreOutput: Boolean): Unit = {
    // LOG.info("LE => " + msgData)
    try {
      // BUGBUG:: for now handling only CSV input data.
      val msg = createMsg(msgType, msgFormat, msgData)
      if (msg != null) {
        // BUGBUG::Get Previous History (through Key) of the top level message/container 
        // Get top level Msg for the current msg
        val topMsgTypeAndHasParent = GetTopMsgName(msgType)
        val partKeyData = msg.PartitionKeyData
        val isValidPartitionKey = (partKeyData != null && partKeyData.size > 0)
        val partitionKeyData = if (isValidPartitionKey) {
          val key = ("PartKey" -> partKeyData.toList)
          compact(render(key))
        } else {
          ""
        }
        val topObj = if (isValidPartitionKey) envContext.getObject(tempTransId, topMsgTypeAndHasParent._1, partitionKeyData) else null
        var handleMsg: Boolean = true
        if (topMsgTypeAndHasParent._2) {
          handleMsg = topObj != null
        }
        if (handleMsg) {
          val finalTopMsgOrContainer: MessageContainerBase = if (topObj != null) topObj else msg
          if (topMsgTypeAndHasParent._2)
            finalTopMsgOrContainer.AddMessage(topMsgTypeAndHasParent._3.parents.toArray, msg)
          var allMdlsResults: scala.collection.mutable.Map[String, ModelResult] = null
          if (isValidPartitionKey && finalTopMsgOrContainer != null) {
            allMdlsResults = envContext.getModelsResult(tempTransId, partitionKeyData)
            if (allMdlsResults == null)
              allMdlsResults = scala.collection.mutable.Map[String, ModelResult]()
          }
          // Run all models
          val results = RunAllModels(tempTransId, finalTopMsgOrContainer, envContext)
          if (results.size > 0) {
            var elapseTmFromRead = (System.nanoTime - readTmNs) / 1000

            if (elapseTmFromRead < 0)
              elapseTmFromRead = 1
            // Prepare final output and update the models persistance map
            results.foreach(res => {
              // Update uniqKey, uniqVal, xformedMsgCntr & totalXformedMsgs
              res.uniqKey = uk
              res.uniqVal = uv
              res.xformedMsgCntr = xformedMsgCntr
              res.totalXformedMsgs = totalXformedMsgs
              allMdlsResults(res.mdlName) = res
            })

            val json =
              ("ModelsResult" -> results.toList.map(res =>
                ("EventDate" -> res.eventDate) ~
                  ("ExecutionTime" -> res.executedTime) ~
                  ("DataReadTime" -> Utils.SimpDateFmtTimeFromMs(rdTmMs)) ~
                  ("ElapsedTimeFromDataRead" -> elapseTmFromRead) ~
                  ("ModelName" -> res.mdlName) ~
                  ("ModelVersion" -> res.mdlVersion) ~
                  ("uniqKey" -> res.uniqKey) ~
                  ("uniqVal" -> res.uniqVal) ~
                  ("xformedMsgCntr" -> res.xformedMsgCntr) ~
                  ("totalXformedMsgs" -> res.totalXformedMsgs) ~
                  ("output" -> res.results.toList.map(r =>
                    ("Name" -> r.name) ~
                      ("Type" -> r.usage.toString) ~
                      ("Value" -> res.ValueString(r.result))))))
            val resStr = compact(render(json))

            envContext.saveStatus(tempTransId, "Start", true)
            if (isValidPartitionKey && finalTopMsgOrContainer != null) {
              envContext.saveModelsResult(tempTransId, partitionKeyData, allMdlsResults)
            }
            if (ignoreOutput == false) {
              output.foreach(o => {
                o.send(resStr, cntr.toString)
              })
            }
            envContext.saveStatus(tempTransId, "OutAdap", false)
          }
          var latencyFromReadToProcess = (System.nanoTime - readTmNs) / 1000 // Nanos to micros
          if (latencyFromReadToProcess < 0) latencyFromReadToProcess = 40 // taking minimum 40 micro secs
          totalLatencyFromReadToProcess += latencyFromReadToProcess
          //BUGBUG:: Save the whole message here
          if (isValidPartitionKey && (topMsgTypeAndHasParent._2 || topObj == null)) {
            envContext.setObject(tempTransId, topMsgTypeAndHasParent._1, partitionKeyData, finalTopMsgOrContainer)
          }
          envContext.saveStatus(tempTransId, "SetData", false)
        }
      } else {
        LOG.error("Recieved null message object for input:" + msgData)
      }
    } catch {
      case e: Exception => LOG.error("Failed to create and run message. Reason:%s Message:%s".format(e.getCause, e.getMessage))
    }

    cntr += 1
  }
}

