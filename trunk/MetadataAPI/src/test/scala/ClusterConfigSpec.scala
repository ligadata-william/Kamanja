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

class ClusterConfigSpec extends CheckAPIPropSpec {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  describe("Test CRUD operations on Cluster Config Objects") {
    it ("Cluster Config Tests") {

      //MetadataAPIImpl.SetLoggerLevel(Level.DEBUG)
      //MetadataAPIImpl.SetLoggerLevel(Level.INFO)
      logger.setLevel(Level.INFO)
      //MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)

      And("The environment variable FATAFAT_HOME must be defined ")
      var fh = System.getenv("FATAFAT_HOME")
      assert(fh != null)

      var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

      And("Check whether CONFIG_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CONFIG_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON config files in " + dirName);
      val cfgFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != cfgFiles.length)

      var fileList = List("ClusterConfig.json")
      fileList.foreach(f1 => {
	And("Add the Config From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  cfgFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("AddConfig first time from " + file.getPath)
	var cfgStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.UploadConfig(cfgStr)
	res should include regex ("\"statusCode\" : 0")

	And("GetAllCfgObjects to fetch all config objects")
	res = MetadataAPIImpl.GetAllCfgObjects("JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetAllNodes to fetch the nodes")
	res = MetadataAPIImpl.GetAllNodes("JSON")
	res should include regex ("\"statusCode\" : 0")
	logger.info(res)

	And("GetAllAdapters to fetch the adapters")
	res = MetadataAPIImpl.GetAllAdapters("JSON")
	res should include regex ("\"statusCode\" : 0")

	And("GetAllClusters to fetch the clusters")
	res = MetadataAPIImpl.GetAllClusters("JSON")
	res should include regex ("\"statusCode\" : 0")

	And("Check number of the nodes")
	var nodes = MdMgr.GetMdMgr.Nodes
	assert(nodes.size == 3)

	And("Check number of the adapters")
	var adapters = MdMgr.GetMdMgr.Adapters
	assert(adapters.size == 4)

	And("RemoveConfig API for the config that was just added")
	res = MetadataAPIImpl.RemoveConfig(cfgStr)
	res should include regex ("\"statusCode\" : 0")

	And("Check number of the nodes after removing config")
	nodes = MdMgr.GetMdMgr.Nodes
	assert(nodes.size == 0)

	And("Check number of the adapters after removing config")
	adapters = MdMgr.GetMdMgr.Adapters
	assert(adapters.size == 0)

	And("AddConfig second time from " + file.getPath)
	cfgStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.UploadConfig(cfgStr)
	res should include regex ("\"statusCode\" : 0")
      })
      MetadataAPIImpl.shutdown
    }
  }
}
