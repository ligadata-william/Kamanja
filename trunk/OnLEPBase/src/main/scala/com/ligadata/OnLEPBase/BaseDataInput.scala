
package com.ligadata.OnLEPBase

trait InputData {
  var dataInput: String
}

class DelimitedData(var dataInput: String, var dataDelim: String) extends InputData {
}

class JsonData(var dataInput: String) extends InputData {
}

class XmlData(var dataInput: String) extends InputData {
}

