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

class TestAllSuites extends FunSuite  {

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  //describe("Execute all UnitTestSpecs in a given order "){
    //it("Unit Tests will be executed in some sequence of APIInitSpec,ContainerSpec,MessageSpec,ModelSpec"){
      var ts1 = new APIInitSpec
      ts1.execute()
    

      //And("Truncate database Before Starting the test")
      val db = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DATABASE")
      assert(null != db)
      MetadataAPIImpl.OpenDbStore(db)
      assert(null != MetadataAPIImpl.GetMetadataStore)
      MetadataAPIImpl.GetMetadataStore.TruncateStore
      assert(null != MetadataAPIImpl.GetConfigStore)
      MetadataAPIImpl.GetConfigStore.TruncateStore
      assert(null != MetadataAPIImpl.GetJarStore)
      MetadataAPIImpl.GetJarStore.TruncateStore
      assert(null != MetadataAPIImpl.GetTransStore)
      MetadataAPIImpl.GetTransStore.TruncateStore
      MetadataAPIImpl.CloseDbStore
      

      //And("Initialize cache with predefined functions, macros, setup datastore connections, setup security/audit adopters")
      var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"
      //And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

      //And("truncate cassandra tables")
      //val res1 = Process("truncate_cas_db.sh").!
      //assert(res1 == 0)

      //And("Container objects..")
      var ts2 = new ContainerSpec
      ts2.execute()
      //And("Message objects..")
      var ts3 = new MessageSpec
      ts3.execute()
      //And("Model objects..")
      //var ts4 = new ModelSpec
      //ts4.execute()
      //And("Type objects..")
      var ts5 = new TypeSpec
      ts5.execute()
      //And("Function objects..")
      var ts6 = new FunctionSpec
      ts6.execute()
      //And("Concept objects..")
      var ts7 = new ConceptSpec
      ts7.execute()
   // }
 // }
}
