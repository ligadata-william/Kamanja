
package com.ligadata.OnLEPBase

import scala.collection.immutable.Map
import com.ligadata.Utils.Utils
import com.ligadata.olep.metadata.MdMgr
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

object MinVarType extends Enumeration {
  type MinVarType = Value
  val Unknown, Active, Predicted, Supplementary, FeatureExtraction = Value
  def StrToMinVarType(tsTypeStr: String): MinVarType = {
    tsTypeStr.toLowerCase.trim() match {
      case "active" => Active
      case "predicted" => Predicted
      case "supplementary" => Supplementary
      case "featureextraction" => FeatureExtraction
      case _ => Unknown
    }
  }
}

import MinVarType._

class Result(val name: String, val usage: MinVarType, val result: Any) {
}

class ModelResult(val eventDate: Long, val executedTime: String, val mdlName: String, val mdlVersion: String, val results: Array[Result]) {
  var uniqKey: String = ""
  var uniqVal: String = ""
  var xformedMsgCntr = 0 // Current message Index, In case if we have multiple Transformed messages for a given input message
  var totalXformedMsgs = 0 // Total transformed messages, In case if we have multiple Transformed messages for a given input message
  def ValueString(v: Any): String = {
    if (v.isInstanceOf[Set[_]]) {
      return v.asInstanceOf[Set[_]].mkString(",")
    }
    if (v.isInstanceOf[List[_]]) {
      return v.asInstanceOf[List[_]].mkString(",")
    }
    if (v.isInstanceOf[Array[_]]) {
      return v.asInstanceOf[Array[_]].mkString(",")
    }
    v.toString
  }

  override def toString: String = {
    val json =
      ("EventDate" -> eventDate) ~
        ("ExecutionTime" -> executedTime) ~
        ("ModelName" -> mdlName) ~
        ("ModelVersion" -> mdlVersion) ~
        ("uniqKey" -> uniqKey) ~
        ("uniqVal" -> uniqVal) ~
        ("xformedMsgCntr" -> xformedMsgCntr) ~
        ("totalXformedMsgs" -> totalXformedMsgs) ~
        ("output" ->
          results.toList.map(r =>
            (("Name" -> r.name) ~
              ("Type" -> r.usage.toString) ~
              ("Value" -> ValueString(r.result)))))
    compact(render(json))
  }

  def toJsonString(readTmNs: Long, rdTmMs: Long): String = {
    var elapseTmFromRead = (System.nanoTime - readTmNs) / 1000

    if (elapseTmFromRead < 0)
      elapseTmFromRead = 1

    val json =
      ("EventDate" -> eventDate) ~
        ("ExecutionTime" -> executedTime) ~
        ("DataReadTime" -> Utils.SimpDateFmtTimeFromMs(rdTmMs)) ~
        ("ElapsedTimeFromDataRead" -> elapseTmFromRead) ~
        ("ModelName" -> mdlName) ~
        ("ModelVersion" -> mdlVersion) ~
        ("uniqKey" -> uniqKey) ~
        ("uniqVal" -> uniqVal) ~
        ("xformedMsgCntr" -> xformedMsgCntr) ~
        ("totalXformedMsgs" -> totalXformedMsgs) ~
        ("output" -> results.toList.map(r =>
          ("Name" -> r.name) ~
            ("Type" -> r.usage.toString) ~
            ("Value" -> ValueString(r.result))))
    compact(render(json))
  }
}

trait EnvContext {
  def Shutdown: Unit
  def SetClassLoader(cl: java.lang.ClassLoader): Unit
  def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, containerNames: Array[String], loadAllData: Boolean, statusInfoStoreType: String, statusInfoSchemaName: String, statusInfoLocation: String): Unit
  def getAllObjects(tempTransId: Long, containerName: String): Array[MessageContainerBase]
  def getObject(tempTransId: Long, containerName: String, key: String): MessageContainerBase
  def setObject(tempTransId: Long, containerName: String, key: String, value: MessageContainerBase): Unit

  def contains(tempTransId: Long, containerName: String, key: String): Boolean
  def containsAny(tempTransId: Long, containerName: String, keys: Array[String]): Boolean
  def containsAll(tempTransId: Long, containerName: String, keys: Array[String]): Boolean

  // Adapters Keys & values
  def setAdapterUniqueKeyValue(tempTransId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit
  def getAdapterUniqueKeyValue(tempTransId: Long, key: String): (String, Int, Int)
  def getAllIntermediateStatusInfo: Array[(String, (String, Int, Int))] // Get all Status information from intermediate table. No Transaction required here.
  def getIntermediateStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] // Get Status information from intermediate table for given keys. No Transaction required here.
  def getAllFinalStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] // Get Status information from Final table. No Transaction required here.
  def saveStatus(tempTransId: Long, status: String, persistIntermediateStatusInfo: Boolean): Unit // Saving Status

  // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
  def saveModelsResult(tempTransId: Long, key: String, value: scala.collection.mutable.Map[String, ModelResult]): Unit
  def getModelsResult(tempTransId: Long, key: String): scala.collection.mutable.Map[String, ModelResult]

  // Final Commit for the given transaction
  def commitData(tempTransId: Long): Unit

  // Save State Entries on local node & on Leader
  def PersistLocalNodeStateEntries: Unit
  def PersistRemainingStateEntriesOnLeader: Unit

  // Clear Intermediate results before Restart processing
  def clearIntermediateResults: Unit

  // Set Reload Flag
  def setReloadFlag(tempTransId: Long, containerName: String): Unit

  def PersistValidateAdapterInformation(validateUniqVals: Array[(String, String)]): Unit
  def GetValidateAdapterInformation: Array[(String, String)]
}

trait ModelBase {
  val gCtx: EnvContext
  val msg: MessageContainerBase
  val modelName: String
  val modelVersion: String
  val tenantId: String
  val tempTransId: Long

  def getModelName: String = modelName // Model Name
  def getVersion: String = modelVersion // Model Version
  def getTenantId: String = tenantId // Tenant Id
  def getTempTransId: Long = tempTransId // tempTransId

  def execute(outputDefault: Boolean): ModelResult // if outputDefault is true we will output the default value if nothing matches, otherwise null 
}

trait ModelBaseObj {
  def IsValidMessage(msg: MessageContainerBase): Boolean // Check to fire the model
  def CreateNewModel(tempTransId: Long, gCtx: EnvContext, msg: MessageContainerBase, tenantId: String): ModelBase // Creating same type of object with given values 

  def getModelName: String // Model Name
  def getVersion: String // Model Version
}

class MdlInfo(val mdl: ModelBaseObj, val jarPath: String, val dependencyJarNames: Array[String], val tenantId: String) {
}

