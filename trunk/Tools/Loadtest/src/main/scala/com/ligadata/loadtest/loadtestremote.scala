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
