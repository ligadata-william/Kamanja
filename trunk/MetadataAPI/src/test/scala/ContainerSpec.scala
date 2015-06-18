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

class ContainerSpec extends FunSpec with GivenWhenThen {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  describe("Test CRUD operations on Container Objects") {
    it ("Container Tests") {

      //MetadataAPIImpl.SetLoggerLevel(Level.DEBUG)
      //MetadataAPIImpl.SetLoggerLevel(Level.INFO)
      //logger.setLevel(Level.INFO)
      //MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)

      //var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"
      //And("The configfile " + myConfigFile + " should exist ")
      //val fl = new File(myConfigFile)
      //assert(fl.exists == true)

      //And("Initialize everything including related to MetadataAPI execution")
      //MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

      And("Check whether CONTAINER_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CONTAINER_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON container files in " + dirName);
      val contFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != contFiles.length)

      //var fileList = List("CoughCodes.json","EnvCodes.json","DyspnoeaCodes.json","SmokeCodes.json","SputumCodes.json")
      var fileList = List("CoughCodes.json")
      fileList.foreach(f1 => {
	And("Add the Container From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  contFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("AddContainer first time from " + file.getPath)
	var contStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddContainer(contStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetContainerDef API to fetch the container that was just added")
	var objName = f1.stripSuffix(".json").toLowerCase
	var version = "0000000000001000000"
	res = MetadataAPIImpl.GetContainerDef("system",objName,"JSON",version)
	res should include regex ("\"statusCode\" : 0")

	And("RemoveContainer API for the container that was just added")
	res = MetadataAPIImpl.RemoveContainer(objName,1000000)
	res should include regex ("\"statusCode\" : 0")

	And("AddContainer again to add Container from " + file.getPath)
	contStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddContainer(contStr,"JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetContainerDef API to fetch  the container that was just added")
	objName = f1.stripSuffix(".json").toLowerCase
	res = MetadataAPIImpl.GetContainerDef("system",objName,"JSON",version)
	res should include regex ("\"statusCode\" : 0")

	And("Get the container object from the cache")
	var o = MdMgr.GetMdMgr.Container("system",objName, version.toLong, true)
	assert(o != None )

	And("Deactivate container that was just added")
	MetadataAPIImpl.DeactivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Get the active container object from the cache after deactivating")
	o = MdMgr.GetMdMgr.Container("system",objName, version.toLong, true)
	assert(o == None )

	And("Make sure the container object from the cache nolonger active ")
	o = MdMgr.GetMdMgr.Container("system",objName, version.toLong, false)
	assert(o != None )

	And("Activate container that was just deactivated")
	MetadataAPIImpl.ActivateObject(o.get.asInstanceOf[BaseElemDef])

	And("Make sure the container object from the cache is active")
	o = MdMgr.GetMdMgr.Container("system",objName, version.toLong, true)
	assert(o != None )

	And("Clone the input json and update the version number to simulate a container for an update operation")
	contStr = contStr.replaceFirst("01.00","01.01")
	assert(contStr.indexOf("\"Version\":\"00.01.01\"") >= 0)
	res = MetadataAPIImpl.UpdateContainer(contStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetContainerDef API to fetch the container that was just updated")
	var newVersion = "0000000000001000001"
	res = MetadataAPIImpl.GetContainerDef("system",objName,"JSON",newVersion)
	res should include regex ("\"statusCode\" : 0")

	And("Get the active container object from the cache after updating")
	o = MdMgr.GetMdMgr.Container("system",objName, newVersion.toLong, true)
	assert(o != None )

	And("Make sure old(pre update version) container object nolonger active after the update")
	o = MdMgr.GetMdMgr.Container("system",objName, version.toLong, true)
	assert(o == None )

      })

      MetadataAPIImpl.shutdown
    }
  }
}
