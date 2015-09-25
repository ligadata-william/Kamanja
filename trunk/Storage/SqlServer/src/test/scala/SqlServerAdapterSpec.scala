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

package com.ligadata.automation.unittests.sqlserveradapter

import org.scalatest._
import Matchers._

import com.ligadata.Utils._
import util.control.Breaks._
import scala.io._
import java.util.Date
import java.io._

import sys.process._
import org.apache.log4j._

import com.ligadata.keyvaluestore._
import com.ligadata.StorageBase._
import com.ligadata.Serialize._
import com.ligadata.Utils.Utils._
import com.ligadata.Utils.{ KamanjaClassLoader, KamanjaLoaderInfo }
import com.ligadata.StorageBase.StorageAdapterObj

case class Customer(name:String, address: String, homePhone: String)

class SqlServerAdapterSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with GivenWhenThen {
  var res : String = null;
  var statusCode: Int = -1;
  var adapter:DataStore = null
  var serializer:Serializer = null

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.INFO)

  private val kvManagerLoader = new KamanjaLoaderInfo

  override def beforeAll = {
    try {
      logger.info("starting...");

      serializer = SerializerManager.GetSerializer("kryo")
      logger.info("Initialize SqlServerAdapter")
      val jarPaths = "/media/home2/installKamanja2/lib/system,/media/home2/installKamanja2/lib/application"
      val dataStoreInfo = """{"StoreType": "sqlserver","hostname": "192.168.56.1","database": "bofa","user":"bofauser","password":"bofauser","jarpaths":"/media/home2/java_examples/sqljdbc_4.0/enu","jdbcJar":"sqljdbc4.jar"}"""
      adapter = SqlServerAdapter.CreateStorageAdapter(kvManagerLoader, dataStoreInfo)
      //adapter = KeyValueManager.Get(jarPaths, dataStoreInfo)
   }
    catch {
      case e: Exception => throw new Exception("Failed to execute set up properly\n" + e)
    }
  }

  describe("Unit Tests for all sqlserveradapter operations") {

    // validate property setup
    it ("Validate api operations") {
      val containerName = "sys.customer1"

      And("Test drop container")
      noException should be thrownBy {
	var containers = new Array[String](0)
	containers = containers :+ containerName
	adapter.DropContainer(containers)
      }

      And("Test create container")
      noException should be thrownBy {
	var containers = new Array[String](0)
	containers = containers :+ containerName
	adapter.CreateContainer(containers)
      }

      And("Test Put api")

      for( i <- 1 to 10 ){
	var currentTime = new Date()
	var keyArray = new Array[String](0)
	var custName = "customer-" + i
	keyArray = keyArray :+ custName
	var key = new Key(currentTime,keyArray,i)
	var custAddress = "1000" + i + ",Main St, Redmond WA 98052"
	var custNumber = "4256667777" + i
	var obj = new Customer(custName,custAddress,custNumber)
	var v = serializer.SerializeObjectToByteArray(obj)
	var value = new Value("kryo",v)
	noException should be thrownBy {
	  adapter.put(containerName,key,value)
	}
      }

      And("Test Del api")
      var keys = new Array[Key](0)
      for( i <- 1 to 10 ){
	var currentTime = new Date()
	var keyArray = new Array[String](0)
	var custName = "customer-" + i
	keyArray = keyArray :+ custName
	var key = new Key(currentTime,keyArray,i)
	keys = keys :+ key
      }
      noException should be thrownBy {
	adapter.del(containerName,keys)
      }
    }
  }
  override def afterAll = {
  }
}
