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

import java.io.File
import com.ligadata.MetadataAPI.MetadataAPIImpl
import com.ligadata.Serialize.SerializerManager
import com.ligadata.Exceptions.AlreadyExistsException
import com.ligadata.kamanja.metadata.MdMgr
import com.ligadata.keyvaluestore.KeyValueManager

import kafka.admin.AdminUtils
import kafka.common.TopicExistsException
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient

import scala.io.Source

/**
 * Created by will on 10/27/15.
 */
object Clean {

  private var clusterCfg: String = ""
  private var zkConnString: String = ""

  private def initialize(configFile: String): Unit = {
    // Some initialization here
    MetadataAPIImpl.InitMdMgrFromBootStrap(configFile, false)
    clusterCfg = MetadataAPIImpl.GetAllAdapters
    zkConnString = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("ZOOKEEPER_CONNECT_STRING")

  }

  def getIOAdapterList(): Unit = {

  }

  def deleteKafkaTopic(topicName: String): Unit = {
    val zkClient:ZkClient = new ZkClient(zkConnString, 6000, 6000, ZKStringSerializer)
    AdminUtils.deleteTopic(zkClient, topicName)
  }

  def main(args: Array[String]): Unit ={
    initialize("/tmp/Kamanja/config/MetadataAPIConfig.properties")
    println("ZOOKEEPER CONNECT STRING: " + zkConnString)
    deleteKafkaTopic("testin_1")
    deleteKafkaTopic("testout_1")
    deleteKafkaTopic("teststatus_1")

  }
}
