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
import com.ligadata.keyvaluestore._
import com.ligadata.StorageBase.DataStore

object CleanStores {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  def cleanMetadata(dataStore: DataStore): Unit = {
    logger.info("Dropping tables from metadata...")
    dataStore.DropContainer(Array("model_config_objects", "metadata_objects", "transaction_id", "jar_store", "config_objects"))
  }

  def cleanDatastore(dataStore: DataStore, tables: Option[Array[String]]): Unit = {
    logger.info("Dropping tables from data store...")
    var tableArr = Array("checkpointinformation", "globalcounters", "modelresults", "adapteruniqkvdata")
    tables match {
      case Some(tableArray) => tableArr = tableArr ++ tableArray
      case None =>
    }
    dataStore.DropContainer(tableArr)
  }

  def cleanStatusInfo(dataStore: DataStore): Unit = {
    logger.info("Dropping tables from ")
  }

}
