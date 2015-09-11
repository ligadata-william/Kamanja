package com.ligadata.jpmml.evaluation

import java.util.{List => JList}
import com.ligadata.KamanjaBase._
import com.ligadata.jpmml.deployment.JpmmlModelManager
import org.dmg.pmml.{FieldName, OpType, DataType}
import org.jpmml.evaluator._

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MutMap}

trait JpmmlEvaluator {
  self: JpmmlModelManager =>
  val DEFAULT_NAME = FieldName.create("_result")

  class JpmmlModel(val modelContext: ModelContext, val modelName: String, val version: String) extends ModelBase {
    override def execute(outputDefault: Boolean): ModelResultBase = {
      val msg = modelContext.msg
      val maybeModelEvaluator = retrieveModelEvaluator(modelName, version)
      maybeModelEvaluator.fold(throw new RuntimeException(s"No evaluator found"))(modelEvaluator => {
        evaluateModel(msg, modelEvaluator)
      })
    }

    private def evaluateModel(jm: MessageContainerBase, modelEvaluator: ModelEvaluator[_]): ModelResultBase = {
      val activeFields = modelEvaluator.getActiveFields
      val preparedFields = prepareFields(activeFields, jm, modelEvaluator)
      val evalResults = replaceNull(modelEvaluator.evaluate(preparedFields.asJava).asScala)
      val results = EvaluatorUtil.decode(evalResults.asJava).asScala
      new MappedModelResults().withResults(results.toArray)
    }

    private def replaceNull[V](evalResults: MutMap[FieldName, V]): MutMap[FieldName, V] = {
      evalResults.get(null.asInstanceOf[FieldName]).fold(evalResults)(v => {
        evalResults - null.asInstanceOf[FieldName] + (DEFAULT_NAME -> v)
      })
    }

    private def prepareFields(activeFields: JList[FieldName], msg: MessageContainerBase, me: ModelEvaluator[_]): Map[FieldName, FieldValue] = {
      activeFields.asScala.foldLeft(Map.empty[FieldName, FieldValue])((map, activeField) => {
        val fieldValue = EvaluatorUtil.prepare(me, activeField, msg.get(activeField.getValue))
        map.updated(activeField, fieldValue)
      })
    }
  }

  class JpmmlModelFactory(val modelName: String, val version: String) extends ModelFactory {
    override def isValidMessage(msg: MessageContainerBase): Boolean = true

    override def createNewModel(modelContext: ModelContext): ModelBase = new JpmmlModel(modelContext, modelName, version)

    override def createResultObject(): ModelResultBase = new MappedModelResults
  }

}