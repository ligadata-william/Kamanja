/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.utils.clean

import com.ligadata.MetadataAPI._
import com.ligadata.StorageBase.DataStore
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.kamanja.metadata.{MdMgr, AdapterInfo, ClusterCfgInfo}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class ZooKeeperInfo(nodeBasePath: String, connStr: String)
//case class DataStore(storeType: String, hostname: String, schemaName: String)
//case class StatusInfo(storeType: String, hostname: String, schemaName: String)
//case class MetadataStore(storeType: String, hostname: String, schemaName: String)

class CleanerConfiguration(metadataConfigFile: String) {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  var zookeeperInfo: ZooKeeperInfo = null
  var dataStore: DataStore = null
  var statusInfo: DataStore = null
  var metadataStore: DataStore = null
  var topicList: List[String] = List()

  // Constructor
  try {
    println("CLEAN-UTIL: Constructor entered. Calling MetadataAPIImpl.InitMdMgrFromBootStrap")
    MetadataAPIImpl.InitMdMgrFromBootStrap(metadataConfigFile, false)
    initialize
  }
  catch {
    case e: Exception => {
      shutdown
      throw new Exception("CLEAN-UTIL: Failed to initialize with exception:\n" + e)
    }
  }
  // End Constructor

  private def getAdapterList(): Array[AdapterInfo] = {
    val adapters = MdMgr.GetMdMgr.Adapters.values.toArray
    return adapters
  }

  private def getTopicNames: List[String] = {
    logger.info("CLEAN-UTIL: Retrieving topic names from cluster configuration")
    var topicNames:List[String] = List()
    getAdapterList().foreach(adapter => {
      val json = parse(adapter.AdapterSpecificCfg)
      val topicName: String = (json \ "TopicName").values.toString
      if(!topicNames.contains(topicName))
        topicNames = topicNames :+ topicName
    })
    if(topicNames.length == 0)
      logger.error("CLEAN-UTIL: Failed to retrieve topic names from cluster configuration. Please ensure you've uploaded cluster configuration to metadata")
    topicNames
  }

  private def initialize(): Unit = {
    logger.info("CLEAN-UTIL: Retrieving Zookeeper Node Base Path from cluster configuration")
    val clusterCfgs: Array[ClusterCfgInfo] = MdMgr.GetMdMgr.ClusterCfgs.values.toArray
    clusterCfgs.foreach(clusterCfg => {
      // Retrieving Zookeeper configuration
      val zkJson = parse(clusterCfg.CfgMap("ZooKeeperInfo"))
      val nodeBasePath = (zkJson \ "ZooKeeperNodeBasePath").values.toString
      val connStr: String = (zkJson \ "ZooKeeperConnectString").values.toString
      if (zkJson == "None" || nodeBasePath == "None" || connStr == "None")
        throw new Exception("CLEAN-UTIL: Failed to retrieve zookeeper configuration. Please ensure you've uploaded cluster configuration to metadata.")
      zookeeperInfo = new ZooKeeperInfo(nodeBasePath, connStr)
      // End Zookeeper configuration

      // Retrieving the Jar Paths to be passed into KeyValueManager
      val tmpJarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS")
      val jarPaths: Set[String] = if (tmpJarPaths != null) tmpJarPaths.split(",").toSet else scala.collection.immutable.Set[String]()

      // Get DataStore storage adapter
      dataStore = KeyValueManager.Get(jarPaths, clusterCfg.cfgMap("DataStore"))

      // Get StatusInfo storage adapter
      statusInfo = KeyValueManager.Get(jarPaths, clusterCfg.cfgMap("StatusInfo"))

      // Get MetadataStore storage adapter
      val mdDataStoreConfig = MetadataAPIImpl.metadataAPIConfig.getProperty("METADATA_DATASTORE")
      metadataStore = KeyValueManager.Get(jarPaths, mdDataStoreConfig)
      //metadataStore = MetadataAPIImpl.GetMainDS

      topicList = getTopicNames
    })
  }

  def shutdown: Unit = {
    MetadataAPIImpl.shutdown
    dataStore.Shutdown
    statusInfo.Shutdown
    metadataStore.Shutdown
  }

}
