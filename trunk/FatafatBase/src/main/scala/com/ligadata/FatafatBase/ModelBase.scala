
package com.ligadata.FatafatBase

import scala.collection.immutable.Map
import com.ligadata.Utils.Utils
import com.ligadata.fatafat.metadata.MdMgr
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

object ModelResult {
  def builder: ModelResultBuilder = null
  def ValueString(v: Any): String = {
    if (v == null) {
      return "null"
    }
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
}

class ModelResultBuilder {
  def build: ModelResult = null;
  def withResult(obj: Any): ModelResultBuilder = null;
}

class ModelResult(val eventDate: Long, val executedTime: String, val mdlName: String, val mdlVersion: String, val results: Array[Result]) {
  var uniqKey: String = ""
  var uniqVal: String = ""
  var xformedMsgCntr = 0 // Current message Index, In case if we have multiple Transformed messages for a given input message
  var totalXformedMsgs = 0 // Total transformed messages, In case if we have multiple Transformed messages for a given input message

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
              ("Value" -> ModelResult.ValueString(r.result)))))
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
            ("Value" -> ModelResult.ValueString(r.result))))
    compact(render(json))
  }
}

trait EnvContext {
  def getRecent(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[MessageContainerBase]
  def getRDD(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase]
  def saveOne(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit
  def saveRDD(transId: Long, containerName: String, values: Array[MessageContainerBase]): Unit

  def Shutdown: Unit
  def SetClassLoader(cl: java.lang.ClassLoader): Unit
  def SetMetadataResolveInfo(mdres: MdBaseResolveInfo): Unit
  def AddNewMessageOrContainers(mgr: MdMgr, storeType: String, dataLocation: String, schemaName: String, databasePrincipal: String, databaseKeytab: String, containerNames: Array[String], loadAllData: Boolean, statusInfoStoreType: String, statusInfoSchemaName: String, statusInfoLocation: String, statusInfoPrincipal: String, statusInfoKeytab: String): Unit
  def getAllObjects(transId: Long, containerName: String): Array[MessageContainerBase]
  def getObject(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase
  def getHistoryObjects(transId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] // if appendCurrentChanges is true return output includes the in memory changes (new or mods) at the end otherwise it ignore them.
  def setObject(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit

  def contains(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): Boolean
  def containsAny(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean //partKeys.size should be same as primaryKeys.size  
  def containsAll(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean //partKeys.size should be same as primaryKeys.size

  // Adapters Keys & values
  def setAdapterUniqueKeyValue(transId: Long, key: String, value: String, xformedMsgCntr: Int, totalXformedMsgs: Int): Unit
  def getAdapterUniqueKeyValue(transId: Long, key: String): (String, Int, Int)
  def getAllIntermediateStatusInfo: Array[(String, (String, Int, Int))] // Get all Status information from intermediate table. No Transaction required here.
  def getIntermediateStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] // Get Status information from intermediate table for given keys. No Transaction required here.
  def getAllFinalStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] // Get Status information from Final table. No Transaction required here.
  def saveStatus(transId: Long, status: String, persistIntermediateStatusInfo: Boolean): Unit // Saving Status

  // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
  def saveModelsResult(transId: Long, key: List[String], value: scala.collection.mutable.Map[String, ModelResult]): Unit
  def getModelsResult(transId: Long, key: List[String]): scala.collection.mutable.Map[String, ModelResult]

  // Final Commit for the given transaction
  def commitData(transId: Long): Unit

  // Save State Entries on local node & on Leader
  def PersistLocalNodeStateEntries: Unit
  def PersistRemainingStateEntriesOnLeader: Unit

  // Clear Intermediate results before Restart processing
  def clearIntermediateResults: Unit

  // Clear Intermediate results After updating them on different node or different component (like KVInit), etc
  def clearIntermediateResults(unloadMsgsContainers: Array[String]): Unit

  // Set Reload Flag
  def setReloadFlag(transId: Long, containerName: String): Unit

  def PersistValidateAdapterInformation(validateUniqVals: Array[(String, String)]): Unit
  def GetValidateAdapterInformation: Array[(String, String)]

  /**
   *  Answer an empty instance of the message or container with the supplied fully qualified class name.  If the name is
   *  invalid, null is returned.
   *  @param fqclassname : a full package qualifed class name
   *  @return a MesssageContainerBase of that ilk
   */
  def NewMessageOrContainer(fqclassname: String): MessageContainerBase
}

abstract class ModelBase(val modelContext: ModelContext, val factory: ModelBaseObj) {
  final def EnvContext() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.gCtx else null // gCtx
  final def ModelName() = factory.ModelName() // Model Name
  final def Version() = factory.Version() // Model Version
  final def TenantId() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.tenantId else null // Tenant Id
  final def TransId() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.transId else null // transId

  def execute(outputDefault: Boolean): ModelResult // if outputDefault is true we will output the default value if nothing matches, otherwise null 
}

trait ModelBaseObj {
  def IsValidMessage(msg: MessageContainerBase): Boolean // Check to fire the model
  def CreateNewModel(mdlCtxt: ModelContext): ModelBase // Creating same type of object with given values 
  def ModelName(): String // Model Name
  def Version(): String // Model Version
}

class MdlInfo(val mdl: ModelBaseObj, val jarPath: String, val dependencyJarNames: Array[String], val tenantId: String) {
}

class ModelContext(val txnContext: TransactionContext, val msg: MessageContainerBase) {
}

class TransactionContext(val transId: Long, val gCtx: EnvContext, val tenantId: String) {
}

