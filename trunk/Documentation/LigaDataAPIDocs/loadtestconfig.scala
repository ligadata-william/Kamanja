package com.ligadata.loadtest

import java.util.concurrent._
import scala.concurrent.duration.Duration
import com.ligadata.keyvaluestore.PropertyMap

class LoadTestConfig
{
	val bRemote = true;
  
	val remotehosts = Array();
	
	// We create an byte array of nRequests * nMaxSize
	val nWorkers = 5
	val nrOfMessagesInTheSystem = 100;
	val nrOfMessages = 1000000
	val nMinMessage = 128
	val nMaxMessage = 256
	val bDoStorage = true;
	val nScenario = 1;
	val nMsgDelay = Duration(1, TimeUnit.MILLISECONDS)
	
	val connectinfo = new PropertyMap
	connectinfo+= ("connectiontype" -> "cassandra")
	connectinfo+= ("hostlist" -> "localhost") 
	connectinfo+= ("schema" -> "default")
	connectinfo+= ("table" -> "default")
}

