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

package com.ligadata.loadtestcommon

import java.util.concurrent._
import scala.concurrent.duration.Duration
import akka.serialization._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.JsonDSL._
import org.json4s.DefaultFormats
import scala.math.max
import scala.math.min

class Configuration extends scala.Serializable
{	
	var Name : String = "default"
	var nScenario : Int = 1
	
	var nWorkers : Int = 10
	var nrOfMessagesInTheSystem : Int = 1000
	var nrOfMessages : Int = 1000
	var nMinMessage : Int = 128
	var nMaxMessage : Int = 256
	
	var bDoStorage : Boolean = true
	var bTruncateStore : Boolean = true
	
	var nMsgDelayMSec : Long = 1
	var nStatusEverySec : Long = 10
	
	var connectinfo = "{\"connectiontype\": \"cassandra\", \"hostlist\": \"localhost\", \"schema\": \"default\"}"
	var tablename = "default"
	
	def Dump()
	{
		println("Workers=" + nWorkers)
		println("MessagesInTheSystem=" + nrOfMessagesInTheSystem)
		println("Messages=" + nrOfMessages)
		println("MinMessage=" + nMinMessage)
		println("MaxMessage=" + nMaxMessage)
		println("DoStorage=" + bDoStorage)
		println("TruncateStore=" + bTruncateStore)
		println("Scenario=" + nScenario)
		println("MsgDelayMSec=" + nMsgDelayMSec)
		println("StatusEverySec=" + nStatusEverySec)
		
		println("Connection info:" + connectinfo)
		println("Table Name:" + tablename)
	}
	
	def DumpJson() : String =
	{
		val json =
			(
				"Configuataions" -> 
				(
					("Name" -> Name) ~
					("Scenario" -> nScenario) ~
					("Workers" -> nWorkers) ~
					("MinMessage" -> nMinMessage) ~
					("MaxMessage" -> nMaxMessage)
				)
			)
	
		return compact(render(json))
	}

	def Clone() : Configuration = 
	{
		var clone = new Configuration
		
		clone.Name = Name
		clone.nScenario = nScenario
		
		clone.nWorkers = nWorkers
		clone.nrOfMessagesInTheSystem = nrOfMessagesInTheSystem
		clone.nrOfMessages = nrOfMessages
		clone.nMinMessage = nMinMessage
		clone.nMaxMessage = nMaxMessage
				
		clone.bDoStorage = bDoStorage
		clone.bTruncateStore = bTruncateStore
		
		clone.nMsgDelayMSec = nMsgDelayMSec
		clone.nStatusEverySec = nStatusEverySec
		
		clone.connectinfo = connectinfo
		clone.tablename = tablename
		
		return clone
	}
}


// Trouble with implicit's - so use as class and use
//
class JValueHelper(node : JValue)  
{
	def Extract(attribute : String, defaultValue : String) : String =
	{		
		node.findField 
		{
			case JField(attr, JString(newvalue)) =>
				if(attr==attribute)
					return newvalue
				false
			case _ => false
		}

		return defaultValue
	}

	def Extract(attribute : String, defaultValue : Int) : Int =
	{
		node.findField 
		{
			case JField(attr, JInt(newvalue)) =>
				if(attr==attribute) 
					return newvalue.toInt 
				false
			case _ => false
		}
		
		return defaultValue
	}
	
	def Extract(attribute : String, defaultValue : Long) : Long =
	{		
		node.findField 
		{
			case JField(attr, JInt(newvalue)) =>
				if(attr==attribute) 
					return newvalue.toLong 
				false
			case _ => false
		}
		
		return defaultValue
	}

	def Extract(attribute : String, defaultValue : Boolean) : Boolean =
	{		
		node.findField 
		{
			case JField(attr, JBool(newvalue)) =>
				if(attr==attribute) 
					return newvalue 
				false
			case _ => false
		}
		
		return defaultValue
	}
}

object Configuration
{

	def Load(jsonfile: String, baseconfig : Configuration) : List[Configuration] =
	{
		val jsonstring = scala.io.Source.fromFile(jsonfile, "utf-8").mkString
		return Configuations(jsonstring)
	}

	def Configuations(jsonstring: String) : List[Configuration] =
	{
			val config = new Configuration
			return Configuations(jsonstring, config) 
	}

	def Configuations(jsonstring: String, baseconfig : Configuration) : List[Configuration] =
	{	
		var configurations = List[Configuration]()
		
		val json = parse(jsonstring)
		
		val JArray(configs) = json \ "configurations"
		
		for(c <- configs)
		{
			val config = new JValueHelper(c)
			var newConfig : Configuration = baseconfig.Clone

			newConfig.Name  = config.Extract("Name", newConfig.Name)
			newConfig.nScenario = config.Extract("Scenario", newConfig.nScenario)
			
			newConfig.nWorkers  = config.Extract("Workers", newConfig.nWorkers)
			newConfig.nrOfMessagesInTheSystem = config.Extract("MessagesInTheSystem", newConfig.nrOfMessagesInTheSystem)
			newConfig.nrOfMessages = config.Extract("Messages", newConfig.nrOfMessages)
			newConfig.nMinMessage = config.Extract("MinMessage", newConfig.nMinMessage)
			newConfig.nMaxMessage = config.Extract("MaxMessage", newConfig.nMaxMessage) 
			
			newConfig.bDoStorage = config.Extract("DoStorage", newConfig.bDoStorage)
			newConfig.bTruncateStore = config.Extract("TruncateStore", newConfig.bTruncateStore)
						
			newConfig.nMsgDelayMSec = min(10000, config.Extract( "MsgDelayMSec", newConfig.nMsgDelayMSec ) ) 
			newConfig.nStatusEverySec = max(1, config.Extract( "StatusEverySec", newConfig.nStatusEverySec) )		
			
			configurations = configurations ::: List(newConfig) 
		}

		return configurations;
	}
}

class RemoteConfiguration extends Configuration
{
	var runners = List[String]() 
	
	override def Dump()
	{
		super.Dump()
		
		println("Runner info")
		runners foreach ( t => println(t) )
	}
	
	override def Clone() : RemoteConfiguration = 
	{
		var clone = new RemoteConfiguration
		
		clone.Name = Name
		clone.nWorkers = nWorkers
		clone.nrOfMessagesInTheSystem = nrOfMessagesInTheSystem
		clone.nrOfMessages = nrOfMessages
		clone.nMinMessage = nMinMessage
		clone.nMaxMessage = nMaxMessage
		clone.bDoStorage = bDoStorage
		clone.nScenario = nScenario
		clone.nMsgDelayMSec = nMsgDelayMSec
		clone.bTruncateStore = bTruncateStore
		clone.nStatusEverySec = nStatusEverySec
		clone.connectinfo = connectinfo
		clone.runners = runners
			
		return clone
	}
	
	def Load(node : JValue) = 
	{
		val config = new JValueHelper(node)

		Name  = config.Extract("Name", Name)
		nScenario = config.Extract("Scenario", nScenario)
		nWorkers  = config.Extract("Workers", nWorkers)
		nrOfMessagesInTheSystem = config.Extract("MessagesInTheSystem", nrOfMessagesInTheSystem)
		nrOfMessages = config.Extract("Messages", nrOfMessages)
		nMinMessage = config.Extract("MinMessage", nMinMessage)
		nMaxMessage = config.Extract("MaxMessage", nMaxMessage) 
		
		bDoStorage = config.Extract("DoStorage", bDoStorage)
		bTruncateStore = config.Extract("TruncateStore", bTruncateStore)
		
		nMsgDelayMSec = min(10000, config.Extract( "MsgDelayMSec", nMsgDelayMSec ) ) 
		nStatusEverySec = max(1, config.Extract( "StatusEverySec", nStatusEverySec) )		
				
		if((node \ "connectioninfo") != JNothing)
		{
			val ci = node \ "connectioninfo"

			ci.findField 
		  	{
		  		case JField(attr, JString(newvalue)) =>
		  			 connectinfo += (attr -> newvalue)
		  			false
		  		case _ => false
		  	}
		}
	}	
}

object RemoteConfiguration
{
	// Load json file and extract the override base configuration and than the configurations  
	//
	def Load(jsonfile: String, baseconfig : RemoteConfiguration) : List[RemoteConfiguration] =
	{
		val jsonstring = scala.io.Source.fromFile(jsonfile, "utf-8").mkString
		
		return Configuations(jsonstring, baseconfig)
	}
	
	// Parse the json and than extract base and list
	//
	def Configuations(jsonstring: String, baseconfig : RemoteConfiguration) : List[RemoteConfiguration] =
	{	
		val json = parse(jsonstring)

		val JObject(config) = json\ "baseconfiguration" 
		
		val newBaseConfig = Configuration(config, baseconfig)
		
	  	val JArray(configs) = json \ "configurations"
	  	
		return  Configurations(configs, newBaseConfig)
	}
	
	// Extract new base configurations
	//
	def Configuration(node : JObject, baseconfig : RemoteConfiguration) : RemoteConfiguration = 
	{
		val config = new JValueHelper(node)
		
		var newConfig = baseconfig.Clone()
		
		newConfig.Load(node)
		
		return newConfig			
	}
	
	// Extract the list with final configurations
	//
	def Configurations(configs : JArray, baseconfig : RemoteConfiguration) : List[RemoteConfiguration] = 
	{
		var configurations = List[RemoteConfiguration]()
		
		for(config <- configs.children)
		{			
			var newConfig : RemoteConfiguration = baseconfig.Clone()
			
			newConfig.Load(config)
			
			configurations = configurations ::: List(newConfig) 
		}

		return configurations
	}
	
}


