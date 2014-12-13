package com.ligadata.testdata

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import scala.collection.mutable.HashMap
import java.io.File
import java.io.PrintWriter

object GenerateJsonData {

  private type OptionMap = Map[Symbol, Any]

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s.charAt(0) == '-')
    list match {
      case Nil => map
      case "--inputfile" :: value :: tail =>
        nextOption(map ++ Map('inputfile -> value), tail)
      case "--outputfile" :: value :: tail =>
        nextOption(map ++ Map('outputfile -> value), tail)
      case "--formatfile" :: value :: tail =>
        nextOption(map ++ Map('formatfile -> value), tail)
      case option :: tail => {
        println("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  def main(args: Array[String]) {

    if (args.length == 0)
      throw new Exception("Please pass the input file as parameter")
    val options = nextOption(Map(), args.toList)

    val inputfile = options.getOrElse('inputfile, null).toString.trim
    if (inputfile == null && inputfile.toString().trim() == "")
      throw new Exception("Please pass the input file as parameter")

    val outputfile = options.getOrElse('outputfile, null).toString.trim
    if (outputfile == null && outputfile.toString().trim() == "")
      throw new Exception("Please pass the input file as parameter")

    val formatfile = options.getOrElse('formatfile, null).toString.trim
    if (formatfile == null && formatfile.toString().trim() == "")
      throw new Exception("Please pass the message format file as parameter")

    var genBen: GenerateJsonData = new GenerateJsonData()
    genBen.processInputData(inputfile.toString, outputfile.toString, formatfile.toString())
  }

}

class GenerateJsonData {

  def processInputData(inputfile: String, outputfile: String, formatfile: String) = {
    try {
      var outfile = new PrintWriter(new File(outputfile))
      var formatMap: Map[String, Array[String]] = Map()
      var formatResult: Array[String] = Array[String]()

      //if ((inputfile == null) || outputfile == null || formatfile == null) throw new Exception(" Error")

      for (formatline <- scala.io.Source.fromFile(new File(formatfile), "utf-8").getLines()) {
        if (formatline != null && formatline.trim.size != 0 && formatline.size > 1) {
          var formatResult = formatline.toLowerCase.split(",", -1)
          formatMap += (formatResult(0).toString() -> formatResult)
        }
      }

      for (line <- scala.io.Source.fromFile(new File(inputfile), "utf-8").getLines()) {

        if (line != null && line.trim.size != 0 && line.size > 1) {
          genData(line, formatMap, outfile)
        }
      }
      outfile.close
    } catch {
      case e: Exception => {
        throw new Exception("Error: " + e.printStackTrace())
      }
    }
  }

  def genData(line: String, formatMap: Map[String, Array[String]], outputfile: PrintWriter) = {
    try {
      var jsonMap: Map[String, String] = Map()
      var finalMap: Map[String, Map[String, String]] = Map()
      var count = 1

      var dataResult = line.split(",", -1);
      var fldData = formatMap.getOrElse(dataResult(0).toLowerCase, null);

      if ((fldData != null) && fldData.isInstanceOf[Array[String]]) {
        if (dataResult.size == fldData.size) {
          var index = 0
          fldData.foreach { fld =>
            if (index > 0)
              jsonMap += (fld -> dataResult(index))
            index = index + 1
          }
          finalMap += (dataResult(0) -> jsonMap)
          writeFile(outputfile, finalMap)
        } else
          println("No MATCH --" + dataResult(0) + " data size " + dataResult.size + " :format size " + fldData.size)
      }
      count = count + 1
    } catch {
      case e: Exception => {
        throw new Exception("Error: " + e.printStackTrace())
      }
    }

  }

  def writeFile(outputfile: PrintWriter, jsonMap: Map[String, Map[String, String]]) = {
    try {

      outputfile.write(compact(render(jsonMap)))
      outputfile.write("\n")
    } catch {
      case e: Exception => {
        throw new Exception("Error: " + e.printStackTrace())
      }
    }

  }

}

