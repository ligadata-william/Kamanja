package com.ligadata.jpmml.deployment

import java.io.{DataOutputStream, DataInputStream}

import com.ligadata.KamanjaBase.{ModelResultBase, TransactionContext, ModelContext}
import com.ligadata.SimpleEnvContextImpl.SimpleEnvContextImpl
import com.ligadata.jpmml.concrete.JpmmlModelEvaluatorConcrete
import com.ligadata.jpmml.message.JpmmlMessage
import org.jpmml.evaluator.ClusterClassificationMap
import org.scalatest.FlatSpec
/**
 * Tests
 */
class JpmmlModelEvaluatorFileReaderTest extends FlatSpec {

  def fixture = new {
    val singleIrisFilePath = createFileNodeList("single_iris_dectree.xml")
    val decisionTreeIrisFilePath = createFileNodeList("decision_tree_iris.pmml")
    val randomForestIrisFilePath = createFileNodeList("random_forest_iris_pmml.xml")
    val kMeansIrisFilePath = createFileNodeList("k_means_iris_pmml.xml")
    val singleIrisName = "single_iris_dectree"
    val decisionTreeIrisName = "decision_tree_iris"
    val randomForestIrisName = "random_forest_iris"
    val kMeansIrisName = "k_means_iris"
    val version = "1.0.0"
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(singleIrisName, version, singleIrisFilePath)
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(decisionTreeIrisName, version, decisionTreeIrisFilePath)
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(randomForestIrisName, version, randomForestIrisFilePath)
    JpmmlModelEvaluatorConcrete.deployModelFromClasspath(kMeansIrisName, version, kMeansIrisFilePath)
  }

  private def createFileNodeList(fileName: String): List[String] = List("jpmmlSample", "metadata", "model", fileName)
  
  "Executing input against decision tree iris model " should " return the right species " in {
    val f = fixture
    val map = createArgsMap(5.1, 3.5, 1.4, 0.2)
    val result = executeInputAndAssertResults(map, f.decisionTreeIrisName)
    assert(result.get("species") === Some("setosa"))
  }

  "Executing input against single iris dectree model " should " return the right class " in {
    val f = fixture
    val map = createArgsMap(5.1, 3.5, 1.4, 0.2)
    val result = executeInputAndAssertResults(map, f.singleIrisName)
    assert(result.get("class") === Some("Iris-setosa"))
  }

  "Executing input against random forest iris model " should " return the right class " in {
    val f = fixture
    val map = createArgsMap(6.4, 2.7, 5.3, 1.9)
    val result = executeInputAndAssertResults(map, f.randomForestIrisName)
    assert(result.get("class") === Some("Iris-virginica"))
  }

  "Executing input against k means iris model " should " return the right class " in {
    val f = fixture
    val map = createArgsMap(6.4, 2.7, 5.3, 1.9)
    val result = executeInputAndAssertResults(map, f.kMeansIrisName)
    assert(result.get("empty").isInstanceOf[ClusterClassificationMap])
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

  private def createArgsMap(sepal_length: Double, sepal_width: Double, petal_length: Double,
                                           petal_width: Double): Map[String, Any] = {
    Map("sepal_length" -> sepal_length, "sepal_width" -> sepal_width, "petal_length" -> petal_length,
      "petal_width" -> petal_width)
  }

}
