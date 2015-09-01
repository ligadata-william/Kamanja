package com.ligadata.jpmml.deployment

import java.io.InputStream

import org.jpmml.evaluator.ModelEvaluator

trait JpmmlModelManager {
  def deployModel(name: String, version: String, is: InputStream)

  def retrieveModelEvaluators(name: String, version: String): Option[List[ModelEvaluator[_]]]
}