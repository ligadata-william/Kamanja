package com.ligadata.loadtest

import com.ligadata.loadtest._

import akka.actor.Actor


class LoadTestLocalStub extends Actor 
{  
	println("start LoadTestLocal")
	
	// Register endpoint
	
	def receive = 
	{
    	case Execute(config: LoadTestConfig) =>
    	{
    		val r = new LoadTestLocal(config, this)
    	}
	}
	
	println("end LoadTestLocal")
}
