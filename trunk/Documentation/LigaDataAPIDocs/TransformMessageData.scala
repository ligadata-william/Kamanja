
package com.ligadata.OnLEPManager

import com.ligadata.OnLEPBase.{ BaseMsg, InputData, DelimitedData, JsonData, XmlData, EnvContext }
import com.ligadata.Utils.Utils

// Bolt to filter data coming from Kafka spout. This takes N input fields and M output fields. Output fields are subset of Input fields.
// We also have KEY. So that we can start sending the user into separate bolts from here.
class TransformMessageData {
  // var cntr: Int = 0

  def parseCsvInputData(inputData: DelimitedData): (String, String) = {
    val str_arr = inputData.dataInput.split(inputData.dataDelim, -1)
    if (str_arr.size == 0)
      throw new Exception("Not found any fields to get Message Type")
    val msgType = str_arr(0).trim

    val msgInfo = OnLEPMetadata.getMessgeInfo(msgType)
    if (msgInfo == null)
      throw new Exception("Not found Message Type \"" + msgType + "\"")

    if (msgInfo.tranformMsgFlds == null) {
      val newArray = new Array[String](str_arr.size - 1)
      Array.copy(str_arr, 1, newArray, 0, str_arr.size - 1)
      val outputData = newArray.mkString(inputData.dataDelim)
      return (msgType, outputData)
    }
    val outputData = msgInfo.tranformMsgFlds.outputFlds.map(fld => {
      if (fld < 0) {
        // throw new Exception("Output Field Idx \"" + fld + "\" not found valid")
        ""
      } else {
        str_arr(fld + 1) // Message type is one extra field in message than Message Definition (inputFields)
      }
    }).mkString(inputData.dataDelim)

    (msgType, outputData)
  }

  def parseJsonInputData(inputData: JsonData): (String, String) = {
    throw new Exception("Not yet handled JSON input data")
    null
  }

  def parseXmlInputData(inputData: XmlData): (String, String) = {
    throw new Exception("Not yet handled XML input data")
    null
  }

  def parseInputData(inputData: InputData): (String, String) = {
    if (inputData.isInstanceOf[DelimitedData])
      return parseCsvInputData(inputData.asInstanceOf[DelimitedData])
    else if (inputData.isInstanceOf[JsonData])
      return parseJsonInputData(inputData.asInstanceOf[JsonData])
    else if (inputData.isInstanceOf[XmlData])
      return parseXmlInputData(inputData.asInstanceOf[XmlData])
    else throw new Exception("Invalid input data type")
  }

  def execute(data: String): (String, String, String) = {
    val output = parseInputData(new DelimitedData(data, ","))
    // cntr += 1
    // if (cntr % 1000 == 0)
    //   SimpleStats.addCntr("F-%10d".format(hashCode()), 1000)
    ("csv", output._1, output._2)
  }
}

