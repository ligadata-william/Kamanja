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

import com.ligadata.loadtest._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool
import akka.actor._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._


class LoadTestRemote(config: LoadTestConfig)  
{  
	println("start LoadTestRemote")
	val context = ActorSystem("LoadTestRemote")
	val bookkeeper = context.actorOf(Props( new RemoteBookeeper), name = "RemoteBookeeper")
	val actor = context.actorOf(Props[LoadTestLocalStub], "remoteActor")
	bookkeeper ! Simulate
	context.awaitTermination()
	println("end LoadTestLocal")
  
	class RemoteBookeeper extends Actor
	{
		def receive = 
		{
			case Simulate =>
			{
				println("recv Simulate")
				actor! Execute(config)
			}
			case Done(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long ) => 
		  	{	  				  	
		  		println("recv Done")
		    	println("\nBookkeeper::Done")
		    	println("ops = " + nOps)		    	
		    	println("t elapse = " + Duration.create(nDurationMs, MILLISECONDS).toString())
		    	println("wrote = " + nWroteBytes)
		    	println("read = " + nReadBytes)
		    	println("delete = " + nDeletesOps)
		    	context.system.shutdown()
		  	}
		}		
	}
}
