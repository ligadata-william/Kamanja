
package com.ligadata.FatafatBase

trait InputData {
  val dataInput: String // Just for Debugging/Logging purpose. Engine will parse the data to corresponding type and give it to populate
}

class DelimitedData(val dataInput: String, val dataDelim: String) extends InputData {
  var tokens: Array[String] = _
  var curPos: Int = _
}

class JsonData(val dataInput: String) extends InputData {
  var root_json: Option[Any] = _
  var cur_json: Option[Any] = _
}

class XmlData(val dataInput: String) extends InputData {
  var root_xml: scala.xml.Elem = _
  var cur_xml: scala.xml.Elem = _
}

