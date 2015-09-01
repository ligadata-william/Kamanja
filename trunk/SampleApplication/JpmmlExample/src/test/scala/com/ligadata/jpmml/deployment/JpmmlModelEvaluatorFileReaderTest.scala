package com.ligadata.jpmml.deployment

import java.io.{DataOutputStream, DataInputStream}

import com.ligadata.KamanjaBase.{ModelResultBase, TransactionContext, ModelContext}
import com.ligadata.SimpleEnvContextImpl.SimpleEnvContextImpl
import com.ligadata.jpmml.concrete.JpmmlModelEvaluatorConcrete
import com.ligadata.jpmml.message.JpmmlMessage
import org.scalatest.FlatSpec
/**
 *
 */
class JpmmlModelEvaluatorFileReaderTest extends FlatSpec {

  def fixture = new {
    val singleIrisFilePath = List("jpmmlSample", "metadata", "model", "single_iris_dectree.xml")
    val decisionTreeIrisFilePath = List("jpmmlSample", "metadata", "model", "decision_tree_iris.pmml")
    val singleIrisName = "single_iris_dectree"
    val decisionTreeIrisName = "decision_tree_iris"
    val version = "1.0.0"
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(singleIrisName, version, singleIrisFilePath)
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(decisionTreeIrisName, version, decisionTreeIrisFilePath)
  }

  "Executing input against decision tree iris model " should " return the right species " in {
    val f = fixture
    val map = createDecisionTreeIrisMsgMap(5.1, 3.5, 1.4, 0.2)
    val result = executeInputAndAssertResults(map, f.decisionTreeIrisName)
    assert(result.get("Species") === Some("setosa"))
  }

  "Executing input against single iris dectree model " should " return the right species " in {
    val f = fixture
    val map = createSingleIrisMsgMap(1, 5.1,3.5,1.4,0.2,"Iris-setosa")
    val result = executeInputAndAssertResults(map, f.singleIrisName)
    assert(result.get("class") === Some("Iris-setosa"))
  }
  
  private def executeInputAndAssertResults(argsMap: Map[String, Any], modelName: String): ModelResultBase = {
    val f = fixture
    val transactionContext = new TransactionContext(0, SimpleEnvContextImpl, "tenantId")
    val message = JpmmlMessage(argsMap)
    val mc = new ModelContext(transactionContext, message)
    val model = new JpmmlModelEvaluatorConcrete.JpmmlModel(mc, modelName, f.version)
    val result = model.execute(true)

    assert(Option(result).isDefined)

    result
  }

  private def createSingleIrisMsgMap(serno: Int, sepal_length: Double, sepal_width: Double, petal_length: Double,
                            petal_width: Double, irisClass: String): Map[String, Any] = {
    Map("serno" -> serno, "sepal_length" -> sepal_length, "sepal_width" -> sepal_width, "petal_length" -> petal_length,
      "petal_width" -> petal_width, "irisClass" -> irisClass)
  }

  private def createDecisionTreeIrisMsgMap(sepal_length: Double, sepal_width: Double, petal_length: Double,
                                     petal_width: Double): Map[String, Any] = {
    Map("Sepal_Length" -> sepal_length, "Sepal_Width" -> sepal_width, "Petal_Length" -> petal_length,
      "Petal_Width" -> petal_width)
  }

}
