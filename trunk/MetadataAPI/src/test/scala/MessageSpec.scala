package com.ligadata.MetadataAPITest

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


class MessageSpec extends FunSpec with GivenWhenThen{
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  describe("Test CRUD operations on Message Objects") {
    it ("Message Tests") {
      //var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"
      //And("The configfile " + myConfigFile + " should exist ")
      //val fl = new File(myConfigFile)
      //assert(fl.exists == true)

      //And("Initialize everything including related to MetadataAPI execution")
      //MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

      And("Check whether MESSAGE_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("MESSAGE_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON message files in " + dirName);
      val msgFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != msgFiles.length)

      //var fileList = List("outpatientclaim.json","inpatientclaim.json","hl7.json","beneficiary.json")
      var fileList = List("outpatientclaim.json")
      fileList.foreach(f1 => {
	And("Add the Message From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  msgFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("AddMessage first time from " + file.getPath)
	var msgStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddMessage(msgStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetMessageDef API to fetch the message that was just added")
	var objName = f1.stripSuffix(".json").toLowerCase
	var version = "0000000000001000000"
	res = MetadataAPIImpl.GetMessageDef("system",objName,"JSON",version)
	res should include regex ("\"statusCode\" : 0")

	And("RemoveMessage API for the message that was just added")
	res = MetadataAPIImpl.RemoveMessage(objName,1000000)
	res should include regex ("\"statusCode\" : 0")

	And("AddMessage again to add Message from " + file.getPath)
	msgStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddMessage(msgStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetMessageDef API to fetch  the message that was just added")
	objName = f1.stripSuffix(".json").toLowerCase
	res = MetadataAPIImpl.GetMessageDef("system",objName,"JSON",version)
	res should include regex ("\"statusCode\" : 0")

	And("Get the message object from the cache")
	var o = MdMgr.GetMdMgr.Message("system",objName, version.toLong, true)
	assert(o != None )

	And("Deactivate message that was just added")
	MetadataAPIImpl.DeactivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Get the active message object from the cache after deactivating")
	o = MdMgr.GetMdMgr.Message("system",objName, version.toLong, true)
	assert(o == None )

	And("Make sure the message object from the cache nolonger active ")
	o = MdMgr.GetMdMgr.Message("system",objName, version.toLong, false)
	assert(o != None )

	And("Activate message that was just deactivated")
	MetadataAPIImpl.ActivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Make sure the message object from the cache is active")
	o = MdMgr.GetMdMgr.Message("system",objName, version.toLong, true)
	assert(o != None )

	And("Clone the input json and update the version number to simulate a message for an update operation")
	msgStr = msgStr.replaceFirst("01.00","01.01")
	assert(msgStr.indexOf("\"Version\":\"00.01.01\"") >= 0)
	res = MetadataAPIImpl.UpdateMessage(msgStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetMessageDef API to fetch the message that was just updated")
	var newVersion = "0000000000001000001"
	res = MetadataAPIImpl.GetMessageDef("system",objName,"JSON",newVersion)
	res should include regex ("\"statusCode\" : 0")

	And("Get the active message object from the cache after updating")
	o = MdMgr.GetMdMgr.Message("system",objName, newVersion.toLong, true)
	assert(o != None )

	And("Make sure old(pre update version) message object nolonger active after the update")
	o = MdMgr.GetMdMgr.Message("system",objName, version.toLong, true)
	assert(o == None )

      })
    
    }
  }
}
