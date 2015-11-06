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

import com.ligadata.Exceptions._

object CleanUtil {
  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  private type OptionMap = Map[Symbol, Any]

  val usage =
    """Usage: CleanUtil --config /path/to/MetadataAPIConfig.properties [--clean-kafka] [--clean-zookeeper] [--clean-testdata [List of messages/containers]] [--clean-metadata] [--cleanstatusinfo]
    """.stripMargin

  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--config" :: value :: tail if !isSwitch(value) =>
        nextOption(map ++ Map('config -> value), tail)
      case "--clean-testdata" :: value :: tail if !isSwitch(value)=>
        nextOption(map ++ Map('cleantestdata -> value), tail)
      case "--clean-testdata" :: tail =>
        nextOption(map ++ Map('cleantestdata -> true), tail)
      case "--clean-kafka" :: tail =>
        nextOption(map ++ Map('cleankafka -> true), tail)
      case "--clean-zookeeper" :: tail =>
        nextOption(map ++ Map('cleanzookeeper -> true), tail)
      case "--clean-metadata" :: tail =>
        nextOption(map ++ Map('cleanmetadata -> true), tail)
      case "--clean-statusinfo" :: tail =>
        nextOption(map ++ Map('cleanstatusinfo -> true), tail)
      case option :: tail => {
        throw new InvalidArgumentException(s"CLEAN-UTIL: Unknown option or invalid usage of option $option.\n$usage")
      }
    }
  }

  def main(args: Array[String]): Unit = {
    var config: CleanerConfiguration = null
    try{
      if(args.length == 0) {
        logger.error("CLEAN-UTIL: No parameters were given.")
        logger.error(usage)
        return
      }
      else {
        val options = nextOption(Map(), args.toList)
        val configFile = options.getOrElse('config, null)
        val cleanKafka = options.getOrElse('cleankafka, false)
        val cleanZookeeper = options.getOrElse('cleanzookeeper, false)
        val cleanTestdata = options.getOrElse('cleantestdata, false)
        val cleanMetadata = options.getOrElse('cleanmetadata, false)
        val cleanStatusinfo = options.getOrElse('cleanstatusinfo, false)

        config = new CleanerConfiguration(configFile.asInstanceOf[String])

        if(cleanKafka.asInstanceOf[Boolean]) {
          config.topicList.foreach(topic => {
            CleanKafka.deleteTopic(topic, config.zookeeperInfo.connStr)
          })
        }

        if(cleanZookeeper.asInstanceOf[Boolean]) {
          CleanZookeeper.deletePath(config.zookeeperInfo)
        }

        if(cleanMetadata.asInstanceOf[Boolean]) {
          CleanStores.cleanMetadata(config.metadataStore)
        }

        if(cleanStatusinfo.asInstanceOf[Boolean]) {
          CleanStores.cleanStatusInfo(config.statusInfo)
        }

        if(cleanTestdata.isInstanceOf[Boolean]) {

          if(cleanTestdata.asInstanceOf[Boolean]) {
            CleanStores.cleanDatastore(config.dataStore, None)
          }
        }
        else {
          CleanStores.cleanDatastore(config.dataStore, Some(cleanTestdata.asInstanceOf[String].split(',')))
        }
      }

    }
    catch {
      case e: MissingArgumentException => logger.error(e)
      case e: InvalidArgumentException => logger.error(e)
      case e: Exception => logger.error("Unexpected Exception caught:\n" + e)
    }
    finally {
      config.shutdown
    }
  }
}
