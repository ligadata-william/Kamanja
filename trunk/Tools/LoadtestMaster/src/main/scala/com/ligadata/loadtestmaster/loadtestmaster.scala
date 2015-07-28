package com.ligadata.loadtestmaster

import com.ligadata.loadtestcommon._
import com.ligadata.loadtestmaster._
import com.ligadata.keyvaluestore._

import com.ligadata.keyvaluestore.PropertyMap

import akka.actor._
import akka.actor.ActorDSL._
import scala.concurrent.duration._
import org.rogach.scallop._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool
import scala.concurrent.duration.Duration
import concurrent._
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j.Logger

class Conf(arguments: Seq[String]) extends ScallopConf(arguments)
{
	val workers = opt[Int](required = false, descr = "Number of workers", default =  Some(10))
	val msg = opt[Int](required = false, descr = "Number of messages" , default =  Some(10001))
	val minsize = opt[Int](required = false, descr = "Minimal size", default =  Some(128))
	val maxsize = opt[Int](required = false, descr = "Maximal size", default =  Some(256))
	val dbhost = opt[String](required = false , descr = "Database address", default =  Some("localhost"))
	val hosts : ScallopOption[List[String]] = trailArg[List[String]](required = true, default =  Some(List[String]("localhost")))
	val scenario = opt[Int](required = false , descr = "Scenario to run", default =  Some(1))
	val confs = opt[String](required = false , descr = "Configuations to run", default =  Some(""))
	val consistencyLevelRead = opt[String](required = false , descr = "Consistency level for read", default =  Some("ONE"))
	val consistencyLevelWrite = opt[String](required = false , descr = "Consistency level for write", default =  Some("ANY"))
	val consistencyLevelDelete = opt[String](required = false , descr = "Consistency level for Delete", default =  Some("ANY"))
	val resultfile = opt[String](required = false , descr = "Where to store the results", default =  Some("result.csv"))
}

class Master(outputfile : String) extends Actor
{
	var nNodes = 0
	var nNodesStarted = 0
	var nNodesCompleted = 0
	var nScenarioMultiplier = 1;
	var startTime : Long = 0
	var endTime : Long = 0

	var nStatusEveryMSec : Long= 10000;

	var sumOps :Long = 0
	var sumWrittenBytes : Long = 0
	var sumReadBytes : Long = 0
	var sumDeletes : Long = 0
	var sumDuration : Long = 0

	var nLastStatusPrint : Long = System.currentTimeMillis
	var nLastOps : Long = 0;
	var nLastSizeW : Long = 0;
	var nLastSizeR : Long = 0;
  private val LOG = Logger.getLogger(getClass)
	var configRemote : RemoteConfiguration = null

	{
		// Create the output file if needed with header
		//
		var file = new java.io.File(outputfile)
		if(!file.exists())
		{
			var outHandle = new java.io.FileOutputStream(outputfile)
			var outStream = new java.io.PrintStream(outHandle)
			outStream.print("name,scenario,nodes,workers,messages,messagesinsystem,minmessage,maxmessage,ops,wbytes,rbytes,deletes,duration,opspersec\n")
			outStream.close();
			outHandle.close();
		}
	}

	def receive =
	{
		case ExecuteMaster(config: RemoteConfiguration) =>
		{
			configRemote = config
			nStatusEveryMSec = config.nStatusEverySec * 1000;

			startTime = System.currentTimeMillis

			if(config.nScenario==1)
				nScenarioMultiplier = 3

			println("\nMaster::Execute")

			config.runners.foreach
			{
				host =>

				try
				{
					nNodesStarted += 1
					val runner = context.actorSelection("akka.tcp://TestRunner@" + host + ":2553/user/TestRunner")
					println("That 's Master:" + runner)
					runner ! Execute(config)
				}
				catch
				{
					case e: Exception => {
            val stackTrace = StackTrace.ThrowableTraceString(e)
            LOG.error("StackTrace:"+stackTrace)
            println("Caught exception " + e.getMessage() + "\n" + "StackTrace:"+stackTrace)}
					self ! Done(0, 0, 0, 0, 0)
				}
			}
		}
		case Execute(config: Configuration) =>
		{
			println("\nMaster::Execute")

			startTime = System.currentTimeMillis
			nStatusEveryMSec = config.nStatusEverySec * 1000;

			val runner = context.actorSelection("akka.tcp://TestRunner@192.168.2.9:2553/user/TestRunner")
			println("That 's Master:" + runner)
			runner ! Execute(config)

		}
		case SoFar(nOps : Long, dummy_nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, dummy_nDeletesOps : Long ) =>
		{
			nLastOps += nOps
			nLastSizeW += nWroteBytes
			nLastSizeR += nReadBytes

			if((nLastStatusPrint + nStatusEveryMSec) < System.currentTimeMillis)
			{
				nLastStatusPrint = System.currentTimeMillis
				val nDurationMs = nLastStatusPrint - startTime
				println("%3.1f".format((nLastOps*100.0)/(configRemote.nrOfMessages* nNodesStarted*nScenarioMultiplier)) + "% " + Duration.create(nDurationMs/1000.0, SECONDS).toString() + " ops: " + nLastOps + " thoughput: " + "%4.1f".format (nLastOps / (nDurationMs/1000.0)))
			}

		}
		case PreparationDone(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long ) =>
		{
			startTime = System.currentTimeMillis
		}
		case Done(nOps : Long, nDurationMs : Long, nWroteBytes : Long, nReadBytes : Long, nDeletesOps : Long ) =>
		{
			sumOps = sumOps + nOps
			sumWrittenBytes = sumWrittenBytes + nWroteBytes
			sumReadBytes = sumReadBytes + nReadBytes
			sumDeletes = sumDeletes + nDeletesOps
			sumDuration = sumDuration + nDurationMs
			nNodesCompleted+=1

			if(nNodesStarted>1)
			{
				println("\nNode " + sender + " done " + nNodesCompleted + " of " + nNodesStarted)
				println("ops = " + nOps)
				println("t elapse = " + Duration.create(nDurationMs, MILLISECONDS).toString())
				println("wrote = " + nWroteBytes)
				println("read = " + nReadBytes)
				println("delete = " + nDeletesOps)
				println("ops = " + "%4.1f".format(nOps / (nDurationMs/1000.0)))
			}

			if(nNodesStarted==nNodesCompleted)
			{
				endTime = System.currentTimeMillis

				println("\nFinal results nodes")
				println("nodes = " + nNodesCompleted)
				println("ops = " + sumOps)
				println("t sum elapse = " + Duration.create(sumDuration, MILLISECONDS).toString())
				println("t elapse = " + Duration.create(endTime-startTime, MILLISECONDS).toString())
				println("wrote = " + sumWrittenBytes)
				println("read = " + sumReadBytes)
				println("delete = " + sumDeletes)
				println("ops = " + "%4.1f".format(sumOps / ((endTime-startTime)/1000.0)))

				var outHandle = new java.io.FileOutputStream(outputfile, true)
				var outStream = new java.io.PrintStream(outHandle)
				outStream.print(
								configRemote.Name
								+ "," + configRemote.nScenario
								+ "," + nNodesCompleted
								+ "," + configRemote.nWorkers
								+ "," + configRemote.nrOfMessages
								+ "," + configRemote.nrOfMessagesInTheSystem
								+ "," + configRemote.nMinMessage
								+ "," + configRemote.nMaxMessage
								+ "," + sumOps
								+ "," + sumWrittenBytes
								+ "," + sumReadBytes
								+ "," + sumDeletes
								+ "," + (endTime-startTime)
								+ "," + "%4.1f".format(sumOps / ((endTime-startTime)/1000.0))
								+ "\n"
								)
				outStream.close();
				outHandle.close();


				// Stops this actor and all its supervised children
				context.system.shutdown()
				context.stop(self)
			}

		}
		case _ => println("Received unknown msg ")
	}
}

object Master
{
	var resultfile = "result.cvs"
  private val LOG = Logger.getLogger(getClass)
	def Prepare(config : Configuration)
	{
		if(config.bTruncateStore)
		{
			println("Truncate store start - 30 sec wait")

			Thread.sleep(30000L)

			var nTries=3

			while(nTries>0)
			{
				val store = KeyValueManager.Get(config.connectinfo)

				try
				{
					store.TruncateStore()
					println("Truncate store done")
					nTries = 0
				}
				catch
				{
					case e1: Exception=>
					{
				  		nTries-=1

				  		if(nTries>0)
				  		{
                val stackTrace = StackTrace.ThrowableTraceString(e1)
                LOG.error("StackTrace:"+stackTrace)
				  			println("Prepare: Caught exception " + e1.getMessage()+"\nStackTrace:"+stackTrace)
				  			Thread.sleep(30000L)

				  		}
				  		else
				  		{
                val stackTrace = StackTrace.ThrowableTraceString(e1)
                LOG.error("StackTrace:"+stackTrace)
				  			println("Prepare: Caught exception " + e1.getMessage() + "\n" + "StackTrace:"+stackTrace)
				  			throw e1
				  		}
					}
				  	case e: com.datastax.driver.core.exceptions.DriverException =>
				  	{
				  		nTries-=1

				  		if(nTries>0)
				  		{
                val stackTrace = StackTrace.ThrowableTraceString(e)
                LOG.error("StackTrace:"+stackTrace)
				  			println("Prepare: Caught exception " + e.getMessage()+"\nStackTrace:"+stackTrace)
				  			Thread.sleep(30000L)

				  		}
				  		else
				  		{
                val stackTrace = StackTrace.ThrowableTraceString(e)
                LOG.error("StackTrace:"+stackTrace)
				  			println("Prepare: Caught exception " + e.getMessage() + "\n" + "StackTrace:"+stackTrace)
				  			throw e
				  		}
				  	}
					//case e: Exception => println("Prepare: Caught exception " + e.getMessage() + "\n" + e.getStackTraceString)
					//throw e
				}
				finally
				{
					store.Shutdown()
				}
			}
		}
	}

	def Conclude()
	{

	}

	def Run(config : RemoteConfiguration)
	{
		println("start configuaration " + config.Name)

		config.Dump()

		Prepare(config)
		implicit val system = ActorSystem("MasterSystem")
		val master  = system.actorOf(Props(new Master(resultfile)), name = "Master")
		master ! ExecuteMaster(config)
		system.awaitTermination()

		Conclude()
	}

	def main(args: Array[String])
	{
		println("start Master")

		val cmdconf = new Conf(args)

		resultfile = cmdconf.resultfile()

		val config = new RemoteConfiguration;

		config.nWorkers = cmdconf.workers()
		config.nrOfMessages = cmdconf.msg()
		config.nMinMessage = cmdconf.minsize()
		config.nMaxMessage = cmdconf.maxsize()
		config.connectinfo("hostlist") = cmdconf.dbhost()
		config.connectinfo("ConsistencyLevelRead") = cmdconf.consistencyLevelRead()
		config.connectinfo("ConsistencyLevelWrite") = cmdconf.consistencyLevelWrite()
		config.connectinfo("ConsistencyLevelDelete") = cmdconf.consistencyLevelDelete()
		config.nScenario = cmdconf.scenario()
		config.runners = cmdconf.hosts()

		val confsfile : String = cmdconf.confs()

		if(confsfile.length()==0)
		{
			Run(config)
		}
		else
		{
			val configs = RemoteConfiguration.Load(confsfile, config)
			for(config <- configs)
			{
				Run(config)
			}
		}
		println("end Master")
	}
}

