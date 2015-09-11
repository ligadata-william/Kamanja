package com.ligadata.jpmml.evaluation

import java.util.{List => JList}
import com.ligadata.KamanjaBase._
import com.ligadata.jpmml.deployment.JpmmlModelManager
import org.dmg.pmml.{FieldName, OpType, DataType}
import org.jpmml.evaluator._

import scala.collection.JavaConverters._

trait JpmmlEvaluator {
  self: JpmmlModelManager =>

  class JpmmlModel(val modelContext: ModelContext, val modelName: String, val version: String) extends ModelBase {
    override def execute(outputDefault: Boolean): ModelResultBase = {
      val msg = modelContext.msg
      val maybeModelEvaluators = retrieveModelEvaluators(modelName, version)
      maybeModelEvaluators.fold(throw new RuntimeException(s"No evaluator found"))(modelEvaluators => {
        evaluateModel(msg, modelEvaluators)
      })
    }

    private def evaluateModel(jm: MessageContainerBase, modelEvaluators: List[ModelEvaluator[_]]): ModelResultBase = {
      //TODO: Only first evaluator used here
      val modelEvaluator = modelEvaluators.head
      val activeFields = modelEvaluator.getActiveFields
      val preparedFields = prepareFields(activeFields, jm, modelEvaluator)
      val jEvalResults = modelEvaluator.evaluate(preparedFields.asJava)
      val evalResults = jEvalResults.asScala
      val maybeTargetFieldName = Option(modelEvaluator.getTargetField)
      maybeTargetFieldName.fold({
        val results = evalResults.map({
          case (k, v) =>
            val maybeK = Option(k)
            maybeK.fold(Result("empty", v))(key => Result(key.getValue, v))
        })
        new MappedModelResults().withResults(results.toArray)
      })(targetFieldName => {
        val targetValue = evalResults.get(targetFieldName)
        val simplePrediction = targetValue.flatMap({
          case c: Computable => Option(c.getResult)
          case _ => None
        })
        val results =  Array(Result(targetFieldName.getValue, simplePrediction))
        new MappedModelResults().withResults(results)
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