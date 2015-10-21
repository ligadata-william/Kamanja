
/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.KamanjaBase

import scala.collection.immutable.Map
import com.ligadata.Utils.Utils
import com.ligadata.kamanja.metadata.MdMgr
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import java.io.{ DataInputStream, DataOutputStream }

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

case class Result(val name: String, val result: Any)

object ModelsResults {
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

  def Deserialize(modelResults: Array[SavedMdlResult], dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {

  }

  def Serialize(dos: DataOutputStream): Array[SavedMdlResult] = {
    null
  }
}

class SavedMdlResult {
  var mdlName: String = ""
  var mdlVersion: String = ""
  var uniqKey: String = ""
  var uniqVal: String = ""
  var txnId: Long = 0
  var xformedMsgCntr: Int = 0 // Current message Index, In case if we have multiple Transformed messages for a given input message
  var totalXformedMsgs: Int = 0 // Total transformed messages, In case if we have multiple Transformed messages for a given input message
  var mdlRes: ModelResultBase = null

  def withMdlName(mdl_Name: String): SavedMdlResult = {
    mdlName = mdl_Name
    this
  }

  def withMdlVersion(mdl_Version: String): SavedMdlResult = {
    mdlVersion = mdl_Version
    this
  }

  def withUniqKey(uniq_Key: String): SavedMdlResult = {
    uniqKey = uniq_Key
    this
  }

  def withUniqVal(uniq_Val: String): SavedMdlResult = {
    uniqVal = uniq_Val
    this
  }

  def withTxnId(txn_Id: Long): SavedMdlResult = {
    txnId = txn_Id
    this
  }

  def withXformedMsgCntr(xfrmedMsgCntr: Int): SavedMdlResult = {
    xformedMsgCntr = xfrmedMsgCntr
    this
  }

  def withTotalXformedMsgs(totalXfrmedMsgs: Int): SavedMdlResult = {
    totalXformedMsgs = totalXfrmedMsgs
    this
  }

  def withMdlResult(mdl_Res: ModelResultBase): SavedMdlResult = {
    mdlRes = mdl_Res
    this
  }

  def toJson: org.json4s.JsonAST.JObject = {
    val output = if (mdlRes == null) List[org.json4s.JsonAST.JObject]() else mdlRes.toJson
    val json =
      ("ModelName" -> mdlName) ~
        ("ModelVersion" -> mdlVersion) ~
        ("uniqKey" -> uniqKey) ~
        ("uniqVal" -> uniqVal) ~
        ("xformedMsgCntr" -> xformedMsgCntr) ~
        ("totalXformedMsgs" -> totalXformedMsgs) ~
        ("output" -> output)
    return json
  }

  override def toString: String = {
    compact(render(toJson))
  }
}

trait ModelResultBase {
  def toJson: List[org.json4s.JsonAST.JObject]
  def toString: String // Returns JSON string
  def get(key: String): Any // Get the value for the given key, if exists, otherwise NULL
  def asKeyValuesMap: Map[String, Any] // Return all key & values as Map of KeyValue pairs
  def Deserialize(dis: DataInputStream): Unit // Serialize this object
  def Serialize(dos: DataOutputStream): Unit // Deserialize this object
}

// Keys are handled as case sensitive
class MappedModelResults extends ModelResultBase {
  val results = scala.collection.mutable.Map[String, Any]()

  def withResults(res: Array[Result]): MappedModelResults = {
    if (res != null) {
      res.foreach(r => {
        results(r.name) = r.result
      })

    }
    this
  }

  def withResults(res: Array[(String, Any)]): MappedModelResults = {
    if (res != null) {
      res.foreach(r => {
        results(r._1) = r._2
      })
    }
    this
  }

  def withResults(res: scala.collection.immutable.Map[String, Any]): MappedModelResults = {
    if (res != null) {
      res.foreach(r => {
        results(r._1) = r._2
      })
    }
    this
  }

  def withResult(res: Result): MappedModelResults = {
    if (res != null) {
      results(res.name) = res.result
    }
    this
  }

  def withResult(res: (String, Any)): MappedModelResults = {
    if (res != null) {
      results(res._1) = res._2
    }
    this
  }

  def withResult(key: String, value: Any): MappedModelResults = {
    if (key != null)
      results(key) = value
    this
  }

  override def toJson: List[org.json4s.JsonAST.JObject] = {
    val json =
      results.toList.map(r =>
        (("Name" -> r._1) ~
          ("Value" -> ModelsResults.ValueString(r._2))))
    return json
  }

  override def toString: String = {
    compact(render(toJson))
  }

  override def get(key: String): Any = {
    results.getOrElse(key, null)
  }

  override def asKeyValuesMap: Map[String, Any] = {
    results.toMap
  }

  override def Deserialize(dis: DataInputStream): Unit = {
    // BUGBUG:: Yet to implement
  }

  override def Serialize(dos: DataOutputStream): Unit = {
    // BUGBUG:: Yet to implement
  }
}

trait EnvContext {
  var _mgr: MdMgr = _
  def setMdMgr(mgr: MdMgr) : Unit
  def getPropertyValue(clusterId: String, key:String): String
  
  def getRecent(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Option[MessageContainerBase]
  def getRDD(transId: Long, containerName: String, partKey: List[String], tmRange: TimeRange, f: MessageContainerBase => Boolean): Array[MessageContainerBase]
  def saveOne(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit
  def saveRDD(transId: Long, containerName: String, values: Array[MessageContainerBase]): Unit

  def Shutdown: Unit
  def SetClassLoader(cl: java.lang.ClassLoader): Unit
  def SetMetadataResolveInfo(mdres: MdBaseResolveInfo): Unit
  def AddNewMessageOrContainers(dataDataStoreInfo: String, containerNames: Array[String], loadAllData: Boolean, statusDataStoreInfo: String, jarPaths: collection.immutable.Set[String]): Unit
  def getAllObjects(transId: Long, containerName: String): Array[MessageContainerBase]
  def getObject(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): MessageContainerBase
  def getHistoryObjects(transId: Long, containerName: String, partKey: List[String], appendCurrentChanges: Boolean): Array[MessageContainerBase] // if appendCurrentChanges is true return output includes the in memory changes (new or mods) at the end otherwise it ignore them.
  def setObject(transId: Long, containerName: String, partKey: List[String], value: MessageContainerBase): Unit
  def contains(transId: Long, containerName: String, partKey: List[String], primaryKey: List[String]): Boolean
  def containsAny(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean //partKeys.size should be same as primaryKeys.size  
  def containsAll(transId: Long, containerName: String, partKeys: Array[List[String]], primaryKeys: Array[List[String]]): Boolean //partKeys.size should be same as primaryKeys.size

  // Adapters Keys & values
  def setAdapterUniqueKeyValue(transId: Long, key: String, value: String, outputResults: List[(String, String)]): Unit
  def getAdapterUniqueKeyValue(transId: Long, key: String): (Long, String, List[(String, String)])
/*
  def getAllIntermediateStatusInfo: Array[(String, (String, Int, Int))] // Get all Status information from intermediate table. No Transaction required here.
  def getIntermediateStatusInfo(keys: Array[String]): Array[(String, (String, Int, Int))] // Get Status information from intermediate table for given keys. No Transaction required here.
*/
  def getAllAdapterUniqKvDataInfo(keys: Array[String]): Array[(String, (Long, String))] // Get Status information from Final table. No Transaction required here.

  def getAllIntermediateCommittingInfo: Array[(String, (Long, String, List[(String, String)]))] // Getting intermediate committing information. Once we commit we don't have this, because we remove after commit

  def getAllIntermediateCommittingInfo(keys: Array[String]): Array[(String, (Long, String, List[(String, String)]))] // Getting intermediate committing information.

  def removeCommittedKey(transId: Long, key: String): Unit
  def removeCommittedKeys(keys: Array[String]): Unit
  
  // Model Results Saving & retrieving. Don't return null, always return empty, if we don't find
  def saveModelsResult(transId: Long, key: List[String], value: scala.collection.mutable.Map[String, SavedMdlResult]): Unit
  def getModelsResult(transId: Long, key: List[String]): scala.collection.mutable.Map[String, SavedMdlResult]

  // Final Commit for the given transaction
  def commitData(transId: Long, key: String, value: String, outputResults: List[(String, String)]): Unit

  // Save State Entries on local node & on Leader
  def PersistLocalNodeStateEntries: Unit
  def PersistRemainingStateEntriesOnLeader: Unit

  // Clear Intermediate results before Restart processing
  def clearIntermediateResults: Unit

  // Clear Intermediate results After updating them on different node or different component (like KVInit), etc
  def clearIntermediateResults(unloadMsgsContainers: Array[String]): Unit

  def getChangedData(tempTransId: Long, includeMessages:Boolean, includeContainers:Boolean): scala.collection.immutable.Map[String, List[List[String]]]
  def ReloadKeys(tempTransId: Long, containerName: String, keys: List[List[String]]): Unit
  
  // Set Reload Flag
  def setReloadFlag(transId: Long, containerName: String): Unit

  def PersistValidateAdapterInformation(validateUniqVals: Array[(String, String)]): Unit
  def GetValidateAdapterInformation: Array[(String, String)]

  /**
   *  Answer an empty instance of the message or container with the supplied fully qualified class name.  If the name is
   *  invalid, null is returned.
   *  @param fqclassname : a full package qualified class name
   *  @return a MesssageContainerBase of that ilk
   */
  def NewMessageOrContainer(fqclassname: String): MessageContainerBase
}

/**
 * The ModelBase describes the base behavior of all model instances, regardless of their kind.
 * @param modelContext The model context supplied to the associated factory is stored as part of the instance state.
 * @param factory the factory that created the ModelBase derivative.
 */
abstract class ModelBase(val modelContext: ModelContext, val factory: ModelBaseObj) {
    /**
     * Answer the EnvContext that provides access to the persistent storage for models that wish to fetch/store values there
     * during its execution.
     */
  final def EnvContext() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.gCtx else null
    /**
     * Answer the model name.
     */
  final def ModelName() = {
     /** Note that for JPMML models, the factory model name would be the same for ALL JPMML models..
       * namely the JpmmlAdapter. For JPMML, the model definition's model name is used, which is found in the
       * JPMMLInfo in the model context. It gives the precise name.  Same story for the Version().
       * @see Version()
       */
    val jpmmlInfo : JPMMLInfo = modelContext.jpmmlInfo.getOrElse(null)
    val name : String = if (jpmmlInfo != null) jpmmlInfo.modelName else factory.ModelName()
    name
  }
    /**
     * Answer the model version.
      */
  final def Version() = {
    val jpmmlInfo : JPMMLInfo = modelContext.jpmmlInfo.getOrElse(null)
    val version : String = if (jpmmlInfo != null) jpmmlInfo.modelVersion.toString else factory.Version()
    version
  }
    /**
     * Answer the model's owner or tenant. This is useful for cluster accounting in multi-tenant situations..
      */
  final def TenantId() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.tenantId else null
    /**
     * Answer the transaction id for the current model execution.
      */
  final def TransId() = if (modelContext != null && modelContext.txnContext != null) modelContext.txnContext.transId else null // transId

    /**
     * The engine will call the model instance's execute method to process the message it received at CreateNewModel time by its factory.
     * @param outputDefault when true, a model result will always be produced with default values.  If false (ordinary case), output is
     *                      emitted only when the model deems this message worthy of report.  If desired the model may return a 'null'
     *                      for the execute's return value and the engine will not proceed with output processing
     * @return a ModelResultBase derivative or null if there is nothing to report.
     */
  def execute(outputDefault: Boolean): ModelResultBase // if outputDefault is true we will output the default value if nothing matches, otherwise null 
}

/**
 * ModelBaseObj describes the contract for Model factories.
 */
trait ModelBaseObj {
    /**
     * Determine if the supplied message can be consumed by the model(s) that this ModelBaseObj can instantiate.
     * @param msg the message instance that is currently being processed
     * @param jPMMLInfo optional state required for JPMML based models.  For Scala, Java, and PMML models, this field's value is None
     * @return true if the model can process the supplied message
     */
  def IsValidMessage(msg: MessageContainerBase, jPMMLInfo: Option[JPMMLInfo]): Boolean // Check to fire the model
    /**
     * If the message can be processed, the engine will call this method to get an instance of the model.  Depending upon the model
     * characteristics, it will either obtain one from its cache (models that are reusable behave this way), or worse case, instantiate
     * a new model to process the message
     * @param mdlCtxt key information needed by the model to create and intialize itself.
     * @return an instance of the Model that can process the message found in the ModelContext
     */
  def CreateNewModel(mdlCtxt: ModelContext): ModelBase
    /**
     * Answer the name of the model.
     * @return the model name
     */
  def ModelName(): String
    /**
     * Answer the version of the model.
     * @return the model version
     */
  def Version(): String

    /**
     * Create a result object to contain any results the model wishes to report
     * @return a ModelResultBase derivative appropriate for the model
     */
  def CreateResultObject(): ModelResultBase // ResultClass associated the model. Mainly used for Returning results as well as Deserialization
}

/**
 * JPMMLInfo contains the additional information needed to both recognize the need to instantiate a model for a given message presented to the
 * model factory's IsValidMessage method as well as that information to instantiate an instance of the model that will process that message.
 * This state information is collected and maintained for JPMML models only.
 * @see ModelInfo
 * @see ModelContext
 *
 * @param jpmmlText the pmml source that will be sent to the JPMML evaluator factory to create a suitable evaluator to process the message.
 * @param msgConsumed the incoming message's namespace.name.version that this pmml model will consume.  It is mapped to the input fields in the
 *                    PMML model by the ModelAdapter instance.
 * @param modelName the modelNamespace.name of the model that will process the message.  Unlike most model factories, JPMML's model factory handles
 *                  all JPMML models.  We need to know which of the JPMML ModelDef instances is being used.
 * @param modelVersion the model version.
 */
case class JPMMLInfo(val jpmmlText : String, val msgConsumed : String, val modelName : String, val modelVersion : Long)

/**
 * ModelInfo objects are created at cluster startup and cache information required to manage the creation of models and their
 * execution in the Kamanja engine.
 * @param mdl the factory object that will be asked to decide if the current message can be consumed by an instance of a model
 *            that it can create.
 * @param jarPath the location(s) of all jars required to execute models that can be produced by this factory object
 * @param dependencyJarNames the names of the dependency jars that are in fact needed
 * @param tenantId the name of the model owner used for multi-tenancy accounting and security
 * @param jpmmlInfo an optional JPMMLInfo instance that describes additional information needed to manage JPMML evaluated models
 */
class ModelInfo(val mdl: ModelBaseObj
                , val jarPath: String
                , val dependencyJarNames: Array[String]
                , val tenantId: String
                , val jpmmlInfo : Option[JPMMLInfo] = None) {
}

/**
 * A ModelContext is presented to each model instance when a new message is to be processed by that model instance.
 * The current transaction, access to the persistent store, the model owner, the message to be processed and optionally
 * the JPMMLInfo required for the JPMML models is available.
 * @param txnContext the TransactionContext describing the transaction id, global context (persistent store interface) and
 *                   tenant id (used for multi tenancy clusters and the accounting required for that).
 * @param msg the instance of the incoming message to be consumed by the model instance.
 * @param jpmmlInfo for JPMML models, a JPMMLInfo instance that contains JPMML specific state.  For other model representations
 *                  (e.g., Scala, Java, and PMML) this value is None.
 */
class ModelContext(val txnContext: TransactionContext
                   , val msg: MessageContainerBase
                   , val jpmmlInfo : Option[JPMMLInfo]   ) {
  def getPropertyValue(clusterId: String, key:String): String = (txnContext.getPropertyValue(clusterId, key))
}

/**
 * The transaction context contains miscellaneous information needed by the engine and models running on it.
 * @param transId the transaction id for the current message execution
 * @param gCtx the EnvContext, the gateway to persisten storage that can contain values required by the model
 *             as well as a storage place for values the model wishes to save.
 * @param tenantId an identifier to aid in multi-tenant clusters.
 */
class TransactionContext(val transId: Long, val gCtx: EnvContext, val tenantId: String) {
  def getPropertyValue(clusterId: String, key:String): String = {gCtx.getPropertyValue(clusterId, key)}
}

