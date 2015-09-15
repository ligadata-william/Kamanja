package com.ligadata.jpmml.deployment

import java.io.InputStream
import javax.xml.bind.{ValidationEvent, ValidationEventHandler}
import javax.xml.transform.sax.SAXSource

import org.dmg.pmml.PMML
import org.jpmml.evaluator.{ModelEvaluator, ModelEvaluatorFactory}
import org.jpmml.model.{ImportFilter, JAXBUtil}
import org.xml.sax.InputSource
import org.xml.sax.helpers.XMLReaderFactory

import scala.collection.concurrent.TrieMap

/**
 * Use a map to store model evaluators
 */
trait JpmmlModelManagerMapImpl extends JpmmlModelManager {

  private case class ModelNameAndVersion(name: String, version: String)

  private val evaluatorMap = TrieMap.empty[ModelNameAndVersion, ModelEvaluator[_]]

  override def deployModel(name: String, version: String, is: InputStream): Unit = {
    val reader = XMLReaderFactory.createXMLReader()
    reader.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true)
    val filter = new ImportFilter(reader)
    val source = new SAXSource(filter, new InputSource(is))
    val unmarshaller = JAXBUtil.createUnmarshaller
    unmarshaller.setEventHandler(SimpleValidationEventHandler)

    val pmml = unmarshaller.unmarshal(source).asInstanceOf[PMML]
    val modelEvaluatorFactory = ModelEvaluatorFactory.newInstance()
    val modelEvaluator = modelEvaluatorFactory.newModelManager(pmml)
    evaluatorMap.put(ModelNameAndVersion(name, version), modelEvaluator)
  }

  override def retrieveModelEvaluator(name: String, version: String): Option[ModelEvaluator[_]] =
    evaluatorMap.get(ModelNameAndVersion(name, version))

  private object SimpleValidationEventHandler extends ValidationEventHandler {
    def handleEvent(event: ValidationEvent): Boolean = {
      val severity: Int = event.getSeverity
      severity match {
        case ValidationEvent.ERROR => false
        case ValidationEvent.FATAL_ERROR => false
        case _ => true
      }
    }
  }
}
