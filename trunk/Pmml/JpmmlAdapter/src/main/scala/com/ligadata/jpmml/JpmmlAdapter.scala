package com.ligadata.jpmml

import java.util.{List => JList}

import com.ligadata.KamanjaBase._
import com.ligadata.kamanja.metadata._
import org.dmg.pmml.FieldName
import org.jpmml.evaluator._

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutableMap}
import scala.collection.concurrent.TrieMap


/**
 * JpmmlAdapter serves a "shim" between the engine and a JPMML evaluator that will perform the actual message
 * scoring.
 */
class JpmmlAdapter( modelContext: ModelContext, factory : ModelBaseObj, modelEvaluator: ModelEvaluator[_]) extends ModelBase(modelContext,factory) {

    /**
     * The engine will call this method to have the model evaluate the input message and produce a ModelResultBase with results.
     * @param outputDefault when true, a model result will always be produced with default values.  If false (ordinary case), output is
     *                      emitted only when the model deems this message worthy of report.  If desired the model may return a 'null'
     *                      for the execute's return value and the engine will not proceed with output processing
     * @return a ModelResultBase derivative or null (if there is nothing to report and outputDefault is false).
     */
    override def execute(outputDefault: Boolean): ModelResultBase = {
        val msg = modelContext.msg
        evaluateModel(msg)
    }

    /**
     * Prepare the active fields, call the model evaluator, and emit results.
     * @param msg the incoming message containing the values of interest to be assigned to the active fields in the
     *            model's data dictionary.
     * @return
     */
    private def evaluateModel(msg : MessageContainerBase): ModelResultBase = {
        val activeFields = modelEvaluator.getActiveFields
        val preparedFields = prepareFields(activeFields, msg, modelEvaluator)
        val evalResultRaw : MutableMap[FieldName, _] = modelEvaluator.evaluate(preparedFields.asJava).asScala
        val evalResults = replaceNull(evalResultRaw)
        val results = EvaluatorUtil.decode(evalResults.asJava).asScala
        new MappedModelResults().withResults(results.toArray)
    }


    /** Since the decode util for the map results shows that at least one key can possibly be null,
      * let's give a name to it for our results.  This is likely just careful coding, but no harm
      * taking precaution.  This is the tag for the null field:
      */
    val DEFAULT_NAME : FieldName = FieldName.create("Anon_Result")

    /**
     * If there is a missing key (an anonymous result), try to manufacture a key for the map so that 
     * all of the result fields returned are decoded and returned.
     * 
     * @param evalResults a mutable map of the results from an evaluator's evaluate function
     * @tparam V the result type
     * @return a map with any null key replaced with a manufactured field name
     */
    private def replaceNull[V](evalResults: MutableMap[FieldName, V]): MutableMap[FieldName, V] = {
         evalResults.get(null.asInstanceOf[FieldName]).fold(evalResults)(v => {
             evalResults - null.asInstanceOf[FieldName] + (DEFAULT_NAME -> v)
        })
    }

    /**
     * Prepare the active fields in the data dictionary of the model with corresponding values from the incoming
     * message.  Note that currently all field names in the model's data dictionary must exactly match the field names
     * from the message.  There is no mapping capability metadata at this point.
     *
     * NOTE: It is possible to have missing inputs in the message.  The model, if written robustly, has accounted
     * for missingValue and other strategies needed to produce results even with imperfect results. See
     * http://dmg.org/pmml/v4-2-1/MiningSchema.html for a discussion about this.
     *
     * @param activeFields a List of the FieldNames
     * @param msg the incoming message instance
     * @param evaluator the JPMML evaluator that the factory as arranged for this instance that can process the
     *                  input values.
     * @return
     */
    private def prepareFields(activeFields: JList[FieldName]
                              , msg: MessageContainerBase
                              , evaluator: ModelEvaluator[_]) : Map[FieldName, FieldValue] = {
        activeFields.asScala.foldLeft(Map.empty[FieldName, FieldValue])((map, activeField) => {
            val key = activeField.getValue
            Option(msg.get(key)).fold(map)(value => {
                val fieldValue : FieldValue = EvaluatorUtil.prepare(evaluator, activeField, value)
                map.updated(activeField, fieldValue)

            })
        })
    }
}

object JpmmlAdapter extends ModelBaseObj {

    /** Key for modelMsgMap */
    private case class ModelNameVersion(name: String, version: String)
    /** Key for instanceMap */
    private case class ModelMessageThreadId(modelName: String, modelVer: String, msgName : String, msgVer : String, threadId : String)

    /** modelMsgMap is utilized to construct new instances of JPMML models.
      *
      * FIXME: When there are updates to models, or a new model is added to the metadata and activated, or perhaps
      * FIXME: a model is deactivated. the JpmmlModelFactory here needs to be notified of this change.  Perhaps an
      * FIXME: Event is sent here such that the maps can be amended.
      */
    private var modelMsgMap : TrieMap[ModelNameVersion, ModelDef] = TrieMap.empty[ModelNameVersion, ModelDef]

    /** modelMsgMap is utilized to construct new instances of JPMML models.
      *
      * FIXME: When a thread terminates, it might be useful to eliminate any instances that were allocated for that thread
      */
    private var instanceMap : TrieMap[ModelMessageThreadId, Option[ModelBase]] = TrieMap.empty[ModelMessageThreadId,Option[ModelBase]]

    /**
     * Called at cluster startup, this method collects the metadata for the JPMML model definitions.  These definitions
     * contain the source
     * @param modelDefinitions
     * @return true if initialization was successful
     */
    def initializeModelMsgMap(modelDefinitions : Array[ModelDef]) : Boolean = {
        modelDefinitions.foreach(modelDef => {
            modelMsgMap.put(ModelNameVersion(modelDef.FullName,modelDef.Version.toString), modelDef)
        })

        (modelMsgMap != null && modelMsgMap.size >= 0)
    }

    /** See if any of the models consume the supplied message */
    def IsValidMessage(msg: MessageContainerBase, jPMMLInfo: Option[JPMMLInfo]): Boolean = {
        val fullName : String = msg.FullName
        val versionStr : String = msg.Version
        //val completeName : String = s"$fullName.$versionStr"
        val completeName : ModelNameVersion = ModelNameVersion(fullName,versionStr)
        val yum : Boolean = modelMsgMap.contains(completeName)
        yum
    }

    override def CreateNewModel(mc: ModelContext): ModelBase = {
        /** First determine if we have an instance of this model for this thread */
        val threadId : String = Thread.currentThread.getId.toString
        val fullName : String = mc.msg.FullName
        val versionStr : String = mc.msg.Version
        val jpmmlInfo : JPMMLInfo = mc.jpmmlInfo.getOrElse(null)
        /** FIXME: it would be unusual, but check for bad JPMMLInfo sent here */
        val modelName : String = if (jpmmlInfo != null) jpmmlInfo.modelName else "ModelNotFound"
        val modelVer : String = if (jpmmlInfo != null) jpmmlInfo.modelVersion.toString else "modelVersionUnknown"

        val modelInstanceKey : ModelMessageThreadId = ModelMessageThreadId(modelName, modelVer, fullName,versionStr,threadId)
        /** if an instance has been constructed of this model for this instance, use it */
        val optOptModel : Option[Option[ModelBase]] = instanceMap.get(modelInstanceKey)
        val optModel : Option[ModelBase] = optOptModel.getOrElse(None)
        val model : ModelBase = optModel.getOrElse(null)

        val useThisModel : ModelBase = if (model != null) {
            model
        } else {
            val optModelDef : Option[ModelDef] = modelMsgMap.get(ModelNameVersion(fullName,versionStr))
            val modelDef : ModelDef = optModelDef.getOrElse(null)

            if (modelDef != null) {
                /** create an instance of the model here FIXME use the code from the metadata api to build an instance */
                val modelName : String = modelDef.FullName
                val version : String = modelDef.Version.toString

                //val builtModel : ModelBase = new JpmmlAdapter(mc)
                val builtModel : ModelBase = new JpmmlAdapter( mc, this, null)
                /** store the instance in the instance map for future reference */
                instanceMap.put(modelInstanceKey, Some(builtModel))

                builtModel
            } else {
                val jpmmlInfo : JPMMLInfo = mc.jpmmlInfo.getOrElse(null)
                val modelName : String = if (jpmmlInfo != null) s"${jpmmlInfo.modelName}.${jpmmlInfo.modelVersion}" else "unknonwn model"

                //logger.error(s"model could not be built for modelName=$modelName")  /// FIXME add logger to classes
                null
            }
        }
        useThisModel
    }

    /// FIXME : these must return the correct model name based upon the ModelContext for the current model.  We many need
    /// FIXME : to pass the model context from the caller in
    def ModelName(): String = {
        ///this.getClass.getName
        "JpmmlAdapter"
    }
    def Version(): String = {
        "000001.000000.000000"
    }

    def CreateResultObject(): ModelResultBase = new MappedModelResults
}
