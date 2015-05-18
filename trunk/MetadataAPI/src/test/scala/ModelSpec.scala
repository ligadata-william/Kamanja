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

import org.json4s.jackson.JsonMethods._

class ModelSpec extends CheckAPIPropSpec {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"


  
  /**
   * extractNameFromPMML - pull the Application name="xxx" from the PMML doc and construct
   *                       a name  string from it, cloned from APIService.scala
   */
  def extractNameFromPMML (pmmlObj: String): String = {
    var firstOccurence: String = "unknownModel"
    val pattern = """Application[ ]*name="([^ ]*)"""".r
    val allMatches = pattern.findAllMatchIn(pmmlObj)
    allMatches.foreach( m => {
      if (firstOccurence.equalsIgnoreCase("unknownModel")) {
      firstOccurence = (m.group(1))
      }
    })
    return firstOccurence
  }


  describe("Test CRUD operations on Model Objects") {
    it ("Model Tests") {
      var fh = System.getenv("FATAFAT_HOME")
      assert(fh != null)

      var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)


      And("Check whether MODEL_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MODEL_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few pmml model files in " + dirName);
      val modFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".xml"))
      assert(0 != modFiles.length)

      var fileList = List("COPDv1.xml")
      fileList.foreach(f1 => {
	And("Add the Model From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  modFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("Call AddModel MetadataAPI Function to add Model from " + file.getPath)
	var modStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddModel(modStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetModelDef API to fetch the model that was just added")
	// Unable to use fileName to identify the name of the object
	// Use this function to extract the name of the model
	var objName = extractNameFromPMML(modStr).toLowerCase
	assert(objName != "unknownModel")

	var version = "0000000000001000000"
	res = MetadataAPIImpl.GetModelDef("system",objName,"XML",version)
	res should include regex ("\"statusCode\" : 0")

	And("RemoveModel API for the model that was just added")
	res = MetadataAPIImpl.RemoveModel(objName,1000000)
	res should include regex ("\"statusCode\" : 0")

	And("AddModel again to add Model from " + file.getPath)
	//modStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddModel(modStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetModelDef API to fetch  the model that was just added")
	//objName = f1.stripSuffix(".json").toLowerCase
	res = MetadataAPIImpl.GetModelDef("system",objName,"XML",version)
	res should include regex ("\"statusCode\" : 0")

	And("Get the model object from the cache")
	var o = MdMgr.GetMdMgr.Model("system",objName, version.toLong, true)
	assert(o != None )

	And("Deactivate model that was just added")
	MetadataAPIImpl.DeactivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Get the active model object from the cache after deactivating")
	o = MdMgr.GetMdMgr.Model("system",objName, version.toLong, true)
	assert(o == None )

	And("Make sure the model object from the cache nolonger active ")
	o = MdMgr.GetMdMgr.Model("system",objName, version.toLong, false)
	assert(o != None )

	And("Activate model that was just deactivated")
	MetadataAPIImpl.ActivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Make sure the model object from the cache is active")
	o = MdMgr.GetMdMgr.Model("system",objName, version.toLong, true)
	assert(o != None )

	And("Clone the input json and update the version number to simulate a model for an update operation")
	modStr = modStr.replaceFirst("01.00","01.01")
	assert(modStr.indexOf("\"00.01.01\"") >= 0)
	res = MetadataAPIImpl.UpdateModel(modStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetModelDef API to fetch the model that was just updated")
	var newVersion = "0000000000001000001"
	res = MetadataAPIImpl.GetModelDef("system",objName,"XML",newVersion)
	res should include regex ("\"statusCode\" : 0")

	And("Get the active model object from the cache after updating")
	o = MdMgr.GetMdMgr.Model("system",objName, newVersion.toLong, true)
	assert(o != None )

	And("Make sure old(pre update version) model object nolonger active after the update")
	o = MdMgr.GetMdMgr.Model("system",objName, version.toLong, true)
	assert(o == None )

      })
    }
  }
}
