package com.ligadata.metadataapitest

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

class APIInitSpec extends CheckAPIPropSpec {

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)

  describe("Verify very basic operations") {
    it ("Validate serialization, storage , zookeeper operations ") {
      logger.setLevel(Level.INFO)
      noException should be thrownBy {
	val cfg = MetadataAPIImpl.GetMetadataAPIConfig
	val db = cfg.getProperty("DATABASE")
	var fh = System.getenv("FATAFAT_HOME")
	val zkConnStr = cfg.getProperty("ZOOKEEPER_CONNECT_STRING")

	And("Validate creation of a database connection")
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

	And("Validate database connection shutdown")
        store.Shutdown()
	
	And("Check Zookeeper connection")
	val zkPath = cfg.getProperty("ZNODE_PATH")
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
	var myConfigFile = fh + "/input/application1/metadata/config/MetadataAPIConfig.properties"
	MetadataAPIImpl.InitMdMgrFromBootStrap(myConfigFile)

	And("Execute shutdown")
	MetadataAPIImpl.shutdown
      }
    }
  }
}
