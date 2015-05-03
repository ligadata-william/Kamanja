package com.ligadata.MetadataAPITest

import org.scalatest._
import Matchers._

import com.ligadata.MetadataAPI._

import com.ligadata.Utils._
import util.control.Breaks._
import scala.io._
import java.util.Date
import java.io.File


class TypeSpec extends FunSpec with GivenWhenThen {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  describe("Test CRUD operations on Type Objects") {
    it ("Type Tests") {
      var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"

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

      var fileList = List("coreUdfTypeDefs.json")
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

	And("Call AddTypes MetadataAPI Type to add Type from " + file.getPath)
	var typeStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddTypes(typeStr,"JSON")
	res should include regex ("\"statusCode\" : 0")
      })
    }
  }
}
