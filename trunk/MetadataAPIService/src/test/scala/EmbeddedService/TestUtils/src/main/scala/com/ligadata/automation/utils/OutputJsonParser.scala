package com.ligadata.automation.utils

import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods._

/**
 * Created by will on 6/8/15.
 */
object OutputJsonParser {
   val logger = org.apache.log4j.Logger.getLogger(this.getClass)
   implicit val formats = org.json4s.DefaultFormats

   def ParseOutputResult(outputJson: String): Output = {
     val json = parse(outputJson)
     logger.debug("JSON - output:\n" + (json \\ "output"))
     val outputObj = json \\ "output"

     def extractOutput:List[OutputField] = {
       for {
         JObject(output) <- outputObj
         JField("Name", JString(name)) <- output
         JField("Type", JString(typ)) <- output
         JField("Value", JString(value)) <- output
       } yield new OutputField(name, typ, value)
     }

     new Output(extractOutput)
   }
 }
