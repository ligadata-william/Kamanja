package com.ligadata.metadataapitest

import org.scalatest._
import Matchers._

import com.ligadata.MetadataAPI._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadata.MdMgr._

import com.ligadata.Utils._
import util.control.Breaks._
import scala.io._
import java.util.Date
import java.io.File

import sys.process._
import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._
import com.ligadata.Serialize._

import com.ligadata.fatafat.metadataload.MetadataLoad

class FunctionSpec extends CheckAPIPropSpec{
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"
  var resCode:Int = 0
  var funcJsonStr:String = null

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)

  describe("Test CRUD operations on Function Objects") {
    it ("Function Tests") {

      var fh = System.getenv("FATAFAT_HOME")
      assert(fh != null)

      var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)


      And("Check whether FUNCTION_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("FUNCTION_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON function files in " + dirName);
      val funcFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != funcFiles.length)

      var fileList = List("SampleFunctions.json")
      fileList.foreach(f1 => {
	And("Add the Function From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  funcFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("Count the functions before adding sample functions")
	var fcnKeys = MetadataAPIImpl.GetAllFunctionsFromCache(true)
	var fcnKeyCnt = fcnKeys.length
	assert(fcnKeyCnt > 0)

	And("Call AddFunctions MetadataAPI Function to add Function from " + file.getPath)
	var funcListJson = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddFunctions(funcListJson,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetAllFunctionDef API to fetch all the functions that were just added")
	var rs1 = MetadataAPIImpl.GetAllFunctionDefs("JSON")
	assert(rs1._1 != 0)
	assert(rs1._2 != null)

	And("Verify function count after adding sample functions")
	fcnKeys = MetadataAPIImpl.GetAllFunctionsFromCache(true)
	var fcnKeyCnt1 = fcnKeys.length
	assert(fcnKeyCnt1 > 0)

	And("SampleFunctions.json contains only 2 functions and newCount is greater by 2")
	assert(fcnKeyCnt1 - fcnKeyCnt == 2)

	And("Explictly parse the funcJsonStr to identify the functions that were just added")
	implicit val jsonFormats: Formats = DefaultFormats
	var json = parse(funcListJson)
	logger.trace("Parsed the json : " + funcListJson)
	val funcList = json.extract[FunctionList]
	assert(funcList.Functions.length == 2)

	And("RemoveFunction API for all the functions that were just added")
	funcList.Functions.foreach(fcn => {
	  res = MetadataAPIImpl.RemoveFunction(fcn.NameSpace, fcn.Name, fcn.Version.toLong)
	  res should include regex ("\"statusCode\" : 0")
	})

	And("Check whether all the functions are removed")
	funcList.Functions.foreach(fcn => {
	  res = MetadataAPIImpl.GetFunctionDef(fcn.NameSpace, fcn.Name, "JSON",fcn.Version)
	  res should include regex("\"statusCode\" : 0")
	})

	And("Verify function count after removing the sample functions")
	fcnKeys = MetadataAPIImpl.GetAllFunctionsFromCache(true)
	fcnKeyCnt1 = fcnKeys.length
	assert(fcnKeyCnt1 > 0)
	assert(fcnKeyCnt1 - fcnKeyCnt == 0)

	And("AddFunctions MetadataAPI Function again to add Function from " + file.getPath)
	res = MetadataAPIImpl.AddFunctions(funcListJson,"JSON")
	res should include regex ("\"statusCode\" : 0")

	funcList.Functions.foreach(fcn => {
	  var o = MdMgr.GetMdMgr.Functions(fcn.NameSpace,fcn.Name,true,true)
	  assert(o != None )
	})
      })
    }
  }
}
