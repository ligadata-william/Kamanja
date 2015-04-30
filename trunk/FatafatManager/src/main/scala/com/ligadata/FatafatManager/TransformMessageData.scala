
package com.ligadata.FatafatManager

import com.ligadata.FatafatBase.{ BaseMsg, InputData, DelimitedData, JsonData, XmlData, EnvContext }
import com.ligadata.Utils.Utils
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.xml.XML
import scala.xml.Elem

// Bolt to filter data coming from Kafka spout. This takes N input fields and M output fields. Output fields are subset of Input fields.
// We also have KEY. So that we can start sending the user into separate bolts from here.
class TransformMessageData {
  // var cntr: Int = 0

  def parseCsvInputData(inputData: String): (String, String) = {
    val dataDelim = ","
    val str_arr = inputData.split(dataDelim, -1)
    if (str_arr.size == 0)
      throw new Exception("Not found any fields to get Message Type")
    val msgType = str_arr(0).trim

    val msgInfo = FatafatMetadata.getMessgeInfo(msgType)
    if (msgInfo == null)
      throw new Exception("Not found Message Type \"" + msgType + "\"")

    if (msgInfo.tranformMsgFlds == null) {
      val newArray = new Array[String](str_arr.size - 1)
      Array.copy(str_arr, 1, newArray, 0, str_arr.size - 1)
      val outputData = newArray.mkString(dataDelim)
      return (msgType, outputData)
    }
    val outputData = msgInfo.tranformMsgFlds.outputFlds.map(fld => {
      if (fld < 0) {
        // throw new Exception("Output Field Idx \"" + fld + "\" not found valid")
        ""
      } else {
        str_arr(fld + 1) // Message type is one extra field in message than Message Definition (inputFields)
      }
    }).mkString(dataDelim)

    (msgType, outputData)
  }

  def parseJsonInputData(inputData: String): (String, String) = {
    val json = parse(inputData)
    if (json == null || json.values == null)
      throw new Exception("Invalid JSON data : " + inputData)
    val parsed_json = json.values.asInstanceOf[Map[String, Any]]
    if (parsed_json.size != 1)
      throw new Exception("Expecting only one message in JSON data : " + inputData)
    val msgTypeAny = parsed_json.head._1
    if (msgTypeAny == null)
      throw new Exception("MessageType not found in JSON data : " + inputData)
    val msgType = msgTypeAny.toString.trim
    val msgInfo = FatafatMetadata.getMessgeInfo(msgType)
    if (msgInfo == null)
      throw new Exception("Not found Message Type \"" + msgType + "\"")
    (msgType, inputData)
  }

  def parseXmlInputData(inputData: String): (String, String) = {
    val xml = XML.loadString(inputData)
    if (xml == null)
      throw new Exception("Invalid XML data : " + inputData)
    val msgTypeAny = (xml \\ "messagetype")
    if (msgTypeAny == null)
      throw new Exception("MessageType not found in XML data : " + inputData)
    val msgType = msgTypeAny.text.toString.trim
    val msgInfo = FatafatMetadata.getMessgeInfo(msgType)
    if (msgInfo == null)
      throw new Exception("Not found Message Type \"" + msgType + "\"")
    (msgType, inputData)
  }

  def parseInputData(inputData: String, msgFormat: String): (String, String) = {
    if (msgFormat.equalsIgnoreCase("csv"))
      return parseCsvInputData(inputData)
    else if (msgFormat.equalsIgnoreCase("json"))
      return parseJsonInputData(inputData)
    else if (msgFormat.equalsIgnoreCase("xml"))
      return parseXmlInputData(inputData)
    else throw new Exception("Invalid input data type")
  }

  def execute(data: String, format: String): Array[(String, String, String)] = {
    val output = parseInputData(data, format)
    Array((format, output._1, output._2))
  }
}

