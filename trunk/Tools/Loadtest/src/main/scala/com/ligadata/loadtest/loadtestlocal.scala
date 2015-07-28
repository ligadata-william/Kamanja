package com.ligadata.loadtest

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random
import java.util.concurrent._
import akka.actor._
import com.ligadata._
import com.ligadata.keyvaluestore._
import com.ligadata.loadtest._
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j.Logger

class LoadTestLocal(config: LoadTestConfig, externalBookKeeper : LoadTestLocalStub = null)
{  
	///////////////////////////////////////////////////////////////////////////
  
	val store = KeyValueManager.Get(config.connectinfo)
  private val LOG = Logger.getLogger(getClass)

	// We create an byte array of nRequests * nMaxSize
	// Create the resources
	// i-th request start at i-th byte and in nMaxMessage long
	val nRandomBytesNeeded = config.nrOfMessages + config.nMaxMessage
	val values = new Value		
	for(i <- 0 until nRandomBytesNeeded)
		values += scala.util.Random.nextInt.toByte

	val keys = new Key		
	for(i <- 0 until nRandomBytesNeeded)
		keys += scala.util.Random.nextInt.toByte
	
	// Set up the threading system
	//
	val context = ActorSystem()
	val master = context.actorOf(Props(new Master(store, config.nWorkers, config.nrOfMessages, bookkeeper)), name = "Master")
	val bookkeeper = context.actorOf(Props( new Bookkeeper), name = "Bookkeeper")	
	val workerPool1 = context.actorOf(Props(new Worker(keys, values, store)).withRouter(RoundRobinPool(config.nWorkers)), name = "workerPool1")
	val workerPool2 = context.actorOf(Props(new Worker(keys, values, store)).withRouter(RoundRobinPool(config.nWorkers)), name = "workerPool2")
	
	println("start LoadTestLocal")
	master ! Simulate		
	context.awaitTermination()		
	store.Shutdown()		
	println("end LoadTestLocal")
	
	///////////////////////////////////////////////////////////////////////////
	
	class Data(key : Key, value : Value) extends IStorage
	{
		def Key = key
		def Value = value
		def Construct(Key: Key, Value: Value) = {}
	}
	
	class Worker(keys: Key, values: Value, store : DataStore) extends Actor 
	{		
		def GetKey(nIndex : Int, nLength : Int) : Key = 
		{
			var key = new Key
			for(i <- 0 to  nLength)
				key += keys(nIndex+i) 
			return key 
			// return keys.slice(nIndex, nLength).asInstanceOf[Key]
		}

		def GetValue(nIndex : Int, nLength : Int) : Value = 
		{
			var value = new Value
			for(i <- 0 to  nLength)
				value += values(nIndex+i) 
			return value
		    // return values.slice(nIndex, nLength).asInstanceOf[Value]
		}
	  		
		def receive = 
		{			
			case Add(nIndex : Int, nLength : Int) =>
			{
				try
				{
					val start: Long = System.currentTimeMillis	
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if(config.bDoStorage) store.add(o) 
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch 
				{
					case e: Exception  => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"\nStackTrace:"+stackTrace)}
				}
				if(config.nScenario==0) sender ! Result ()
			}
			case Put(nIndex : Int, nLength : Int) => 
			{
				try
				{
					val start: Long = System.currentTimeMillis											
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if(config.bDoStorage) store.put(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch 
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"/nStackTrace:"+stackTrace)
            }
				}
				if(config.nScenario==0) sender ! Result ()
			}
			case Del(nIndex : Int) => 
			{											
				try
				{
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, 0))
					if(config.bDoStorage) store.del(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_D(0, start, end)
				}
				catch 
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"\nStackTrace:"+stackTrace)}
				}					
				if(config.nScenario==0) sender ! Result ()
			}
			case Get(nIndex : Int) => 
			{
				try
				{
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, 0))
					if(config.bDoStorage) store.get(GetKey(nIndex, 8), o)
					val end: Long = System.currentTimeMillis
					sender ! Result_R(0, start, end)
				}
				catch 
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"\nStackTrace:"+stackTrace)}
				}					
				if(config.nScenario==0) sender ! Result ()
			}
			
			// Work for scenario 1
			//
			case S1_put(nIndex : Int, nLength : Int) => 
			{
				try
				{
					val start: Long = System.currentTimeMillis											
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if(config.bDoStorage) store.put(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch 
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"\nStackTrace:"+stackTrace)}
				}

				context.system.scheduler.scheduleOnce(config.nMsgDelay) 
					{ workerPool2 ! S1_get_put(nIndex, nLength) } (scala.concurrent.ExecutionContext.global);

			}			
			case S1_get_put(nIndex : Int, nLength : Int) => 
			{															
				try
				{
					// Do reading
					val start1: Long = System.currentTimeMillis
					val i = new Data(GetKey(nIndex, 8), GetValue(nIndex, 0))
					if(config.bDoStorage) store.get(GetKey(nIndex, 8), i)
					val end1: Long = System.currentTimeMillis
					sender ! Result_R(nLength, start1, end1)
					
					// Do writing
					val start2: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if(config.bDoStorage) store.put(o)
					val end2: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start2, end2)
				}
				catch 
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception"+"\nStackTrace:"+stackTrace)}
				}
				
				sender ! Result()
			}		

			// Send those the messages back to the originator
			//
			case Result() =>
			{				
				master ! Result()
			}			
			case Result_W(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				master ! Result_W(nLength, nStart, nEnd)
			}			
			case Result_R(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				master ! Result_R(nLength, nStart, nEnd)			
			}			
			case Result_D(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				master ! Result_D(nLength, nStart, nEnd)
			}			
			case _ => 
		  	{
		  		println("Worker::Catch-All\n")
		  	}
		}
	}
	
	class Bookkeeper extends Actor 
	{
		def receive = 
		{ 
		  	case Done(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long ) => 
		  	{
		  		context.system.shutdown()		  		
		    	println("\nBookkeeper::Done")
		    	println("ops = " + nOps)		    	
		    	println("t elapse = " + Duration.create(nDurationMs, MILLISECONDS).toString())
		    	println("wrote = " + nWroteBytes)
		    	println("read = " + nReadBytes)
		    	println("delete = " + nDeletesOps)
		  	}
		  	case _ => 
		  	{
		  		println("Bookkeeper::Catch-All\n")
		  	}
		}
	}
		
	class Master(store : DataStore, nrOfWorkers: Int, nrOfMessages: Int, bookkeeper: ActorRef) extends Actor
	{ 
		val taskStart: Long = System.currentTimeMillis
		
		var sendMessages : Int = 0
		var recvMessages : Int = 0

		var recvMessagesW : Int = 0
		var nDurationW : Long = 0
		var nSizeW : Long = 0

		var recvMessagesR : Int = 0
		var nDurationR : Long = 0
		var nSizeR : Long = 0

		var recvMessagesD : Int = 0
		var nDurationD : Long = 0
		var nSizeD : Long = 0
		
		def StartNew() =
		{ 		
			val nLength = config.nMinMessage + ((config.nMaxMessage - config.nMinMessage) *  scala.util.Random.nextFloat()).toInt
				
			if(config.nScenario==1)
				workerPool1 ! S1_put(sendMessages, nLength)
			else
				workerPool1 ! Put(sendMessages, nLength)

			sendMessages+=1
		}
		
		def receive = 
		{ 
			case Simulate =>		
			{			  
				for (i <- 0 until config.nrOfMessagesInTheSystem)
					StartNew()
			}			
			case Result() =>
			{				
				val taskEnd: Long = System.currentTimeMillis
				recvMessages+=1
				
				if (recvMessages == nrOfMessages) 
				{
					// Send the result to the listener
					bookkeeper ! Done(recvMessagesW+recvMessagesR, taskEnd - taskStart, nSizeW, nSizeR, 0)
					
					if(externalBookKeeper!=null)
					  externalBookKeeper.sender ! Done(recvMessagesW+recvMessagesR, taskEnd - taskStart, nSizeW, nSizeR, 0)
					  
					// Stops this actor and all its supervised children
					context.stop(self)
				}				
				else if(nrOfMessages > sendMessages)
					StartNew()
			}
			case Result_W(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				nDurationW += (nEnd-nStart)
				nSizeW += nLength
				recvMessagesW+=1
			}			
			case Result_R(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				nDurationR += (nEnd-nStart)
				nSizeR += nLength
				recvMessagesR+=1
			}			
			case Result_D(nLength: Int, nStart: Long,  nEnd: Long) =>
			{				
				nDurationD += (nEnd-nStart)
				nSizeD += nLength
				recvMessagesD+=1
			}
			case _ => 
		  	{
		  		println("Master::Catch-All\n")
		  	}
		}
	}
}
