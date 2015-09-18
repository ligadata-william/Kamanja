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

package com.ligadata.loadtestrunner

import com.ligadata.loadtestcommon._
import com.ligadata.loadtestrunner._
import com.typesafe.config._
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

import org.rogach.scallop._ 

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) 
{
	val ip = opt[String](required = true, descr = "IP address to listen on")
}

class TestRunner extends Actor
{
	def receive = 
	{
		case Execute(config: Configuration) =>
		{
			println("recv Execute")
			config.Dump()
			val r = new Runner(config, sender)			
			r.Run			
			println("done Execute")
		}
		case Simulate => 
		{
			println("recv Simulate")
			val config = new Configuration;
			val r = new Runner(config, sender)
			println("done Simulate")
		}
		case _ => 
		{
			println("TestRunner::Catch-All\n")
		}
	}
}

object TestRunner
{
	def main(args: Array[String]) 
	{	
		val cmdconf = new Conf(args) 
		val ip : String = cmdconf.ip();
		
		println("start TestRunner ip=" + ip)
		
		val root = ConfigFactory.load()
		val actorconf = ConfigFactory.parseString("akka.remote.netty.tcp.hostname=\"" +  ip + "\"").withFallback(root);
		val system = ActorSystem("TestRunner", actorconf)
		
		val runner = system.actorOf(Props(new TestRunner), name = "TestRunner")
		
		println(runner.path)
		
		system.awaitTermination()

		println("end TestRunner")
	} 
}
