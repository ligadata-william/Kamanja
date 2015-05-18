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

class TestAllSuites extends FunSuite  {

  val loggerName = this.getClass.getName
  val logger = Logger.getLogger(loggerName)

  var ts0 = new CheckAPIPropSpec
  ts0.execute()

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
      
  
  //And("Container objects..")
  var ts1 = new ContainerSpec
  ts1.execute()
  //And("Message objects..")
  var ts2 = new MessageSpec
  ts2.execute()
  //And("Model objects..")
  var ts3 = new ModelSpec
  ts3.execute()
  //And("Type objects..")
  var ts4 = new TypeSpec
  ts4.execute()
  //And("Function objects..")
  var ts5 = new FunctionSpec
  ts5.execute()
  //And("ClusterConfig objects..")
  var ts6 = new ClusterConfigSpec
  ts6.execute()
  //And("MiscFuncSpec..")
  var ts7 = new MiscFuncSpec
  ts7.execute()
}
