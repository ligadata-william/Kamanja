package com.ligadata.MetadataAPITest

import org.scalatest._
import Matchers._

import com.ligadata.MetadataAPI._

import java.io.{ByteArrayOutputStream, _}
import com.datastax.driver.core.Cluster
import com.esotericsoftware.kryo.io.{Input, Output}
import com.ligadata.Serialize._
import com.ligadata.ZooKeeper._
import com.ligadata.keyvaluestore._
import com.ligadata.fatafat.metadata._
import com.ligadata.fatafat.metadataload.MetadataLoad
import com.twitter.chill.ScalaKryoInstantiator

import com.ligadata.ZooKeeper._
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import org.apache.log4j._
import org.apache.zookeeper.CreateMode
import scala.collection.mutable.ArrayBuffer
import scala.io._

class APIInitSpec extends FunSpec with GivenWhenThen {

  var databaseOpen = false
  var serializer = SerializerManager.GetSerializer("kryo")
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  describe("Initialize the environment") {
    it ("Initialize properties from MetadataAPIConfig.properties") {
      var myConfigFile = System.getenv("HOME") + "/MetadataAPIConfig.properties"

      And("The configfile " + myConfigFile + " should exist ")
      val fl = new File(myConfigFile)
      assert(fl.exists == true)
	
      MetadataAPIImpl.readMetadataAPIConfigFromPropertiesFile(myConfigFile)

      And("MetadataAPIImpl.GetMetadataAPIConfig should have been initialized")
      val cfg = MetadataAPIImpl.GetMetadataAPIConfig
      assert(null != cfg)

      And("The property DATABASE must have been defined")
      val db = cfg.getProperty("DATABASE")
      assert(null != db)
      if ( db == "cassandra" ){
	And("The property MetadataLocation must have been defined for store type " + db)
	val loc = cfg.getProperty("DATABASE_LOCATION")
	assert(null != loc)
	And("The property MetadataSchemaName must have been defined for store type " + db)
	val schema = cfg.getProperty("DATABASE_SCHEMA")
	assert(null != schema)
      }
      And("The property NODE_ID must have been defined")
      assert(null != cfg.getProperty("NODE_ID"))  

      
      And("The property JAR_TRAGET_DIR must have been defined")
      val d = cfg.getProperty("JAR_TARGET_DIR")
      assert(null != d)

      And("Make sure the Directory " + d + " exists")
      val f = new File(d)
      assert(null != f)

      And("The property SCALA_HOME must have been defined")
      val sh = cfg.getProperty("SCALA_HOME")
      assert(null != sh)

      And("The property JAVA_HOME must have been defined")
      val jh = cfg.getProperty("SCALA_HOME")
      assert(null != jh)

      And("The property CLASSPATH must have been defined")
      val cp = cfg.getProperty("CLASSPATH")
      assert(null != cp)

      And("The property ZNODE_PATH must have been defined")
      val zkPath = cfg.getProperty("ZNODE_PATH")
      assert(null != zkPath)

      And("The property ZOOKEEPER_CONNECT_STRING must have been defined")
      val zkConnStr = cfg.getProperty("ZOOKEEPER_CONNECT_STRING")
      assert(null != zkConnStr)

      And("The property SERVICE_HOST must have been defined")
      val shost = cfg.getProperty("SERVICE_HOST")
      assert(null != shost)

      And("The property SERVICE_PORT must have been defined")
      val sport = cfg.getProperty("SERVICE_PORT")
      assert(null != sport)

      And("The property JAR_PATHS must have been defined")
      val jp = cfg.getProperty("JAR_PATHS")
      assert(null != jp)

      And("The property SECURITY_IMPL_JAR  must have been defined")
      val sij = cfg.getProperty("SECURITY_IMPL_JAR")
      assert(null != sij)

      And("The property SECURITY_IMPL_CLASS  must have been defined")
      val sic = cfg.getProperty("SECURITY_IMPL_CLASS")
      assert(null != sic)

      And("The property DO_AUTH  must have been defined")
      val da = cfg.getProperty("DO_AUTH")
      assert(null != da)

      And("The property AUDIT_IMPL_JAR  must have been defined")
      val aij = cfg.getProperty("AUDIT_IMPL_JAR")
      assert(null != sij)

      And("The property AUDIT_IMPL_CLASS  must have been defined")
      val aic = cfg.getProperty("AUDIT_IMPL_CLASS")
      assert(null != sic)

      And("The property DO_AUDIT  must have been defined")
      val dau = cfg.getProperty("DO_AUDIT")
      assert(null != dau)

      And("The property SSL_CERTIFICATE  must have been defined")
      val sc = cfg.getProperty("SSL_CERTIFICATE")
      assert(null != sc)

      And("Check database connection")
      val store = MetadataAPIImpl.GetDataStoreHandle(db, "test_store", "test_objects")
      assert(null != store)

      And("Check database save ")
      val serializer = SerializerManager.GetSerializer("kryo")
      val key = "key1"
      var ba = serializer.SerializeObjectToByteArray("value1")
      MetadataAPIImpl.UpdateObject(key,ba,store)

      And("Check database get")
      var obj = MetadataAPIImpl.GetObject("key1",store)
      var v = serializer.DeserializeObjectFromByteArray(obj.Value.toArray[Byte]).asInstanceOf[String]
      assert(v == "value1")

      And("Check Zookeeper connection")
      val znodePath = zkPath + "/metadataupdate"
      CreateClient.CreateNodeIfNotExists(zkConnStr, znodePath)
      val zkc = CreateClient.createSimple(zkConnStr)
      assert(null != zkc)

      And("Bootstrap metadata manager")
      MdMgr.GetMdMgr.truncate
      val mdLoader = new MetadataLoad(MdMgr.mdMgr, "", "", "", "")
      mdLoader.initialize

      And("Setup database connection(handle) to various tables")
      MetadataAPIImpl.OpenDbStore(db)
      assert(null != MetadataAPIImpl.GetMetadataStore)
      assert(null != MetadataAPIImpl.GetConfigStore)
      assert(null != MetadataAPIImpl.GetJarStore)
      assert(null != MetadataAPIImpl.GetTransStore)

      And("Initialize everything including related to MetadataAPI execution")
      MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

    }
  }
}
