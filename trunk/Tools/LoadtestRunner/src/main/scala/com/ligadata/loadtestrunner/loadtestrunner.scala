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
