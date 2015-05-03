package com.ligadata.MetadataAPITest

import org.scalatest._
import Matchers._

import com.ligadata.MetadataAPI._

import com.ligadata.Utils._
import util.control.Breaks._
import scala.io._
import java.util.Date
import java.io.File


class ConceptSpec extends FunSpec with GivenWhenThen {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  describe("Test CRUD operations on Concept Objects") {
    it ("Concept Tests") {
      var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"

      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)


      And("Check whether CONCEPT_FILES_DIR defined as property")
      var dirName = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("CONCEPT_FILES_DIR")
      assert(null != dirName)

      And("Check Directory Path")
      val iFile = new File(dirName)
      assert(true == iFile.exists)

      And("Check whether " + dirName + " is a directory ")
      assert(true == iFile.isDirectory)

      And("Make sure there are few JSON concept files in " + dirName);
      val conceptFiles = new java.io.File(dirName).listFiles.filter(_.getName.endsWith(".json"))
      assert(0 != conceptFiles.length)

      var fileList = List("coreUdfFcnDefs.json")
      fileList.foreach(f1 => {
	And("Add the Concept From " + f1)
	And("Make Sure " + f1 + " exist")
	var exists = false
	var file: java.io.File = null
	breakable{
	  conceptFiles.foreach(f2 => {
	    if( f2.getName() == f1 ){
	      exists = true
	      file = f2
	      break
	    }
	  })
	}
	assert(true == exists)

	And("Call AddConcepts MetadataAPI Concept to add Concept from " + file.getPath)
	var conceptStr = Source.fromFile(file).mkString
	res = MetadataAPIImpl.AddConcepts(conceptStr,"JSON")
	res should include regex ("\"statusCode\" : 0")
      })
    
    }
  }
}
