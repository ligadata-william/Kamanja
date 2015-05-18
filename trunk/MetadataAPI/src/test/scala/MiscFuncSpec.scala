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
import java.io._

import sys.process._
import org.apache.log4j._
import org.json4s.jackson.JsonMethods._

class MiscFuncSpec extends CheckAPIPropSpec {
  var res : String = null;
  var statusCode: Int = -1;
  var apiResKey:String = "\"statusCode\" : 0"

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  describe("Test Other misc operations ") {
    it ("Misc tests not covered by other specs") {

      //MetadataAPIImpl.SetLoggerLevel(Level.DEBUG)
      //MetadataAPIImpl.SetLoggerLevel(Level.INFO)
      logger.setLevel(Level.INFO)
      //MdMgr.GetMdMgr.SetLoggerLevel(Level.TRACE)

      var fh = System.getenv("FATAFAT_HOME")
      var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
      And("The configfile " + myConfigFile + " should exist ")
      var fl = new File(myConfigFile)
      assert(fl.exists == true)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

      var cfg = MetadataAPIImpl.GetMetadataAPIConfig
      assert(null != cfg)

      And("Test Authorization functions")
      And("The property DO_AUTH  must have been defined")
      var da = cfg.getProperty("DO_AUTH")
      assert(null != da)
      
      And("checkAuth with correct user and password")
      var chk = MetadataAPIImpl.checkAuth(Some("lonestarr"),Some("vespa"),Some("goodguy"),"read")
      assert(chk == true)

      And("checkAuth with wrong user")
      chk = MetadataAPIImpl.checkAuth(Some("lonestarr1"),Some("vespa"),Some("goodguy"),"read")
      assert(chk == false)

      And("checkAuth with wrong password")
      chk = MetadataAPIImpl.checkAuth(Some("lonestarr"),Some("vespa1"),Some("goodguy"),"read")
      assert(chk == false)

      And("checkAuth for admin user")
      chk = MetadataAPIImpl.checkAuth(Some("root"),Some("secret"),Some("admin"),"adminprivileges")
      assert(chk == true)

      And("Audit Functions")
      And("The property DO_AUDIT  must have been defined")
      da = cfg.getProperty("DO_AUDIT")
      assert(null != da)

      And("logAuditRec ")
      noException should be thrownBy {
	MetadataAPIImpl.logAuditRec(Some("lonestarr"),Some("write"),"GetContainerDef","system.coughcodes.000000100000000000","true","12345657","system.coughcodes.000000100000000000")
      }

      And("getAuditRec ")
      res = MetadataAPIImpl.getAuditRec(new Date((new Date).getTime() - 1500 * 60000),null,null,null,null)
      assert(res != null)
      res should include regex("\"Action\" : \"GetContainerDef\"")
      res should include regex("\"UserOrRole\" : \"lonestarr\"")
      res should include regex("\"Status\" : \"true\"")
      res should include regex("\"ObjectAccessed\" : \"system.coughcodes.000000100000000000\"")
      res should include regex("\"ActionResult\" : \"system.coughcodes.000000100000000000\"")

      And("GetTranId returns last transactionId used as long")
      var l = MetadataAPIImpl.GetTranId
      assert(l > 0)

      And("GetNewTranId returns a long")
      var l1 = MetadataAPIImpl.GetNewTranId
      assert(l + 1 == l1)

      And("PutTranId updates the tranId")
      noException should be thrownBy {
	MetadataAPIImpl.PutTranId(l1)
      }

      And("GetTranId after last update of transactionId")
      var l2 = MetadataAPIImpl.GetTranId
      assert(l2 == l1)

      And("GetJarAsArrayOfBytes with invalid jarName as input")
      var ex = the [FileNotFoundException] thrownBy {
	var aob = MetadataAPIImpl.GetJarAsArrayOfBytes("unknown_junk.jar")
      }
      ex.getMessage should include regex("Jar file \\(unknown_junk.jar\\) is not found")

      And("GetJarAsArrayOfBytes with valid jarName as input")
      noException should be thrownBy {
	var sampleJarFile = System.getenv("FATAFAT_HOME") + "/lib/system/fatafatbase_2.10-1.0.jar"
	var aob = MetadataAPIImpl.GetJarAsArrayOfBytes(sampleJarFile)
	fl = new File(sampleJarFile)
        assert(aob.length == fl.length())
      }
      MetadataAPIImpl.shutdown
    }
  }
}
