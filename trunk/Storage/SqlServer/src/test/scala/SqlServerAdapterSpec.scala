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

class SqlServerAdapterSpec extends FunSpec with BeforeAndAfter with BeforeAndAfterAll with GivenWhenThen {
  var res : String = null;
  var statusCode: Int = -1;
  var adatapter:SqlServerAdapter = null

  private val loggerName = this.getClass.getName
  private val logger = Logger.getLogger(loggerName)
  logger.setLevel(Level.INFO)

  override def beforeAll = {
    try {

      logger.info("starting...");

      logger.info("Initialize SqlServerAdapter")
      val jarPaths = "/media/home2/installKamanja2/lib/system,/media/home2/installKamanja2/lib/application"
      val dataStoreInfo = {"StoreType": "sqlserver","hostname": "192.168.56.1","database": "bofa","user":"bofauser","password":"bofauser"}
      adapter = KeyValueManager.Get(jarPaths, dataStoreInfo)
   }
    catch {
      case e: Exception => throw new Exception("Failed to execute set up properly\n" + e)
    }
  }

  describe("Unit Tests for all sqlserveradapter operations") {

    // validate property setup
    it ("Validate ") {
      val tableName = "customer"

      val currentTime = new Date()
      var key = new Array[String](0)
      key = key :+ "cust1" 
      And("Test Put api")
    }
  }
  override def afterAll = {
  }
}
