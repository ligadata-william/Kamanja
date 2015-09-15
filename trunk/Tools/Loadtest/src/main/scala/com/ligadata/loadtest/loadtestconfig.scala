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

package com.ligadata.loadtest

import java.util.concurrent._
import scala.concurrent.duration.Duration

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
	val connectinfo = "{\"connectiontype\": \"cassandra\", \"hostlist\": \"localhost\", \"schema\": \"default\"}"
	val tablename = "default"
}


