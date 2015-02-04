package com.ligadata.loadtest

import com.ligadata.loadtest._
import org.rogach.scallop._
import akka.actor._

sealed trait StorageWorklet

// Basic operations
//
case class Add(Index : Int, nLength : Int) extends StorageWorklet
case class Put(Index : Int, nLength : Int) extends StorageWorklet	
case class Get(Index : Int) extends StorageWorklet
case class Del(Index : Int) extends StorageWorklet

// Higher order operations
//
case class S1_put(Index : Int, nLength : Int) extends StorageWorklet
case class S1_get_put(Index : Int, nLength : Int) extends StorageWorklet

// Mgmt 
case class Execute(config: LoadTestConfig) extends StorageWorklet
case class Done(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long) extends StorageWorklet
case object Simulate extends StorageWorklet

case class Result() extends StorageWorklet // Something is done (really)
case class Result_W(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A write is done
case class Result_R(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A read is done
case class Result_D(nLength: Int, nStart: Long,  nEnd: Long ) extends StorageWorklet // A read is done

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) 
{
	val types = opt[String](required = true, descr = "Select type local, remote, client as value ")
}

object LoadTest 
{
	def main(args: Array[String]) 
	{
		println("start main")

		val conf = new Conf(args) 
		
		println("types is: " + conf.types())
		
		// Run local version
		val config = new LoadTestConfig;
		
		if(conf.types() == "local")
		{
			val local = new LoadTestLocal(config)
		}
		else if(conf.types() == "client")
		{
			// Start the actor system to receive messages
		    // Use CTRL-C to exit 
			val system = ActorSystem("ClientSystem")			
		}
		else if(conf.types() == "remote")
		{
			// Run remote version
			val remote = new LoadTestRemote(config)
		}
		else
		{
			println("Unknown type")
		}
		println("end main")
	}
}
