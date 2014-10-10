
package com.ligadata.OnLEPBase

import scala.util.parsing.json.{ JSONObject, JSONArray }
import scala.collection.immutable.Map
import com.ligadata.Utils.Utils
import com.ligadata.olep.metadata.MdMgr

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

class Result(var name: String, var usage: MinVarType, var result: Any) {
}

class ModelResult(var eventDate: Long, var executedTime: String, var mdlName: String, var mdlVersion: String, var results: Array[Result]) {
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
    JSONObject(Map(
      "EventDate" -> eventDate.toString,
      "ExecutionTime" -> executedTime,
      "ModelName" -> mdlName,
      "ModelVersion" -> mdlVersion,
      "output" -> JSONArray(results.map(r => JSONObject(Map(
        "Name" -> r.name,
        "Type" -> r.usage.toString,
        "Value" -> ValueString(r.result)))).toList))).toString
  }

  def toJsonString(readTmNs: Long, rdTmMs: Long): String = {
    var elapseTmFromRead = (System.nanoTime - readTmNs) / 1000
    
    if (elapseTmFromRead < 0)
      elapseTmFromRead = 1

    JSONObject(Map(
      "EventDate" -> eventDate.toString,
      "ExecutionTime" -> executedTime,
      "DataReadTime" -> Utils.SimpDateFmtTimeFromMs(rdTmMs),
      "ElapsedTimeFromDataRead" -> elapseTmFromRead.toString,
      "ModelName" -> mdlName,
      "ModelVersion" -> mdlVersion,
      "output" -> JSONArray(results.map(r => JSONObject(Map(
        "Name" -> r.name,
        "Type" -> r.usage.toString,
        "Value" -> ValueString(r.result)))).toList))).toString
  }
}

trait EnvContext {
  def initContainers(mgr : MdMgr, dataPath : String, containerNames: Array[String]): Unit
  def initMessages(mgr: MdMgr, dataPath: String, msgNames: Array[String]): Unit
  def getObjects(containerName: String, key: String): Array[BaseContainer]
  def getObject(containerName: String, key: String): BaseContainer
  def setObject(containerName: String, key: String, value: BaseContainer): Unit
  def setObject(containerName: String, elementkey: Any, value: BaseContainer): Unit
  def getMsgObject(containerName: String, key: String): BaseMsg
  def setMsgObject(containerName: String, key: String, value: BaseMsg): Unit
}

//val gCtx : com.ligadata.OnLEPBase.EnvContext, val msg : com.ligadata.OnLEPBankPoc.BankPocMsg, val modelName:String, val modelVersion:String, val tenantId: String
trait ModelBase {
  val gCtx: EnvContext
  val msg: BaseMsg
  val modelName: String
  val modelVersion: String
  val tenantId: String

  def getModelName: String = modelName // Model Name
  def getVersion: String = modelVersion // Model Version
  def getTenantId: String = tenantId // Tenant Id

  def execute(outputDefault: Boolean): ModelResult // if outputDefault is true we will output the default value if nothing matches, otherwise null 
}

trait ModelBaseObj {
  def IsValidMessage(msg: BaseMsg): Boolean // Check to fire the model
  def CreateNewModel(gCtx: EnvContext, msg: BaseMsg, tenantId: String): ModelBase // Creating same type of object with given values 

  def getModelName: String // Model Name
  def getVersion: String // Model Version
}

class MdlInfo(var mdl: ModelBaseObj, var jarPath: String, var dependencyJarNames: Array[String], var tenantId: String) {
}

