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

class TypeSpec extends CheckAPIPropSpec {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)

  describe("Test CRUD operations on Type Objects") {
    it ("Type Tests") {

      //logger.setLevel(Level.TRACE)
      //MetadataAPIImpl.SetLoggerLevel(Level.TRACE)
      //JsonSerializer.SetLoggerLevel(Level.TRACE)

      var fh = System.getenv("FATAFAT_HOME")
      assert(fh != null)

      var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)


      And("Check whether TYPE_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("TYPE_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON type files in " + dirName);
      val typeFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != typeFiles.length)

      var fileList = List("SampleTypes.json")
      fileList.foreach(f1 => {
	And("Add the Type From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  typeFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	var typKeys = MetadataAPIImpl.GetAllTypesFromCache(true)
	assert(typKeys.length != 0)

	And("Call AddTypes MetadataAPI Type to add Type from " + file.getPath)
	var typeStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddTypes(typeStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	var resCode:Int = 0
	And("GetAllTypes API to fetch all the types that were just added as json string")
	var typeJsonStr1 = MetadataAPIImpl.GetAllTypes("JSON")
	assert(typeJsonStr1 != null)
	
	And("verify the type count after adding new types")
	var typKeys1 = MetadataAPIImpl.GetAllTypesFromCache(true)
	assert(typKeys1.length != 0)
	assert(typKeys1.length - typKeys.length == 5)

	And("RemoveType API for the types that were just added")
	implicit val jsonFormats: Formats = DefaultFormats
	var json = parse(typeStr)
	//logger.trace("Parsed the json : " + typeStr)
	var typesAdded = json.extract[TypeDefList]

	//var typesAdded = JsonSerializer.parseTypeList(typeStr,"JSON")
	assert(typesAdded.Types.length == 5)

	typesAdded.Types.foreach(typ => {
	  res = MetadataAPIImpl.RemoveType(typ.NameSpace, typ.Name, typ.Version.toLong)
	  res should include regex ("\"statusCode\" : 0")
	})

	And("verify the type count after removing new types")
	typKeys1 = MetadataAPIImpl.GetAllTypesFromCache(true)
	assert(typKeys1.length != 0)
	assert(typKeys.length - typKeys1.length == 0)

	And("Check whether all the types are removed")
	typesAdded.Types.foreach(typ => {
	  res = MetadataAPIImpl.GetTypeDef(typ.NameSpace, typ.Name, "JSON", typ.Version)
	  res should include regex ("\"statusCode\" : 0")
	})


	And("AddTypes again ")
	res = MetadataAPIImpl.AddTypes(typeStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("Check GetType function")
	var typeObjList = new Array[BaseTypeDef](0)
	typesAdded.Types.foreach(typ => {
	  var o = MetadataAPIImpl.GetType(typ.NameSpace, typ.Name, typ.Version,"JSON")
	  assert(o != None )
	  typeObjList = typeObjList :+ o.get
	})
	assert(typeObjList.length == typesAdded.Types.length)

	And("Generate Json String from typeList generated using GetType function")
	var typeJsonStr2 = JsonSerializer.SerializeObjectListToJson("Types",typeObjList)
	//logger.trace(typeJsonStr2)

	And("RemoveType API for the types that were just added")
	var json1 = parse(typeJsonStr2)
	logger.trace("Parsed the json : " + typeJsonStr2)
	var typesAdded1 = json1.extract[TypeDefList]
	assert(typesAdded1.Types.length == 5)
	typesAdded1.Types.foreach(typ => {
	  logger.trace("Remove the type " + Array(typ.NameSpace,typ.Name,typ.Version).mkString("."))
	  res = MetadataAPIImpl.RemoveType(typ.NameSpace, typ.Name, typ.Version.toLong)
	  res should include regex ("\"statusCode\" : 0")
	})

	And("AddTypes again this time use the json string generated by JsonSerializer")
	res = MetadataAPIImpl.AddTypes(typeJsonStr2,"JSON")
	res should include regex ("\"statusCode\" : 0")
      })
    }
  }
}
