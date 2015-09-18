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

package com.ligadata.loadtestcommon

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.ActorSystem
import akka.event.Logging
import akka.event.LoggingAdapter;
import akka.routing.RoundRobinPool
import com.typesafe.config._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.Random
import java.util.concurrent._
import akka.actor._
import com.ligadata._
import com.ligadata.StorageBase.{ DataStore, Transaction, IStorage, Key, Value, StorageAdapterObj }
import com.ligadata.loadtestcommon._
import com.ligadata.keyvaluestore.KeyValueManager
import com.ligadata.Exceptions.StackTrace
import org.apache.log4j.Logger

class Runner(config: Configuration, externalBookKeeper : ActorRef = null) {
	///////////////////////////////////////////////////////////////////////////

	val store = KeyValueManager.Get(collection.immutable.Set[String](), config.connectinfo, config.tablename)

	private val LOG = Logger.getLogger(getClass)
	// We create an byte array of nRequests * nMaxSize
	// Create the resources
	// i-th request start at i-th byte and in nMaxMessage long
	val nRandomBytesNeeded = 2 * config.nrOfMessages + config.nMaxMessage

	val values = new Value
	for (i <- 0 until nRandomBytesNeeded)
		values += scala.util.Random.nextInt.toByte

	val keys = new Key
	for (i <- 0 until nRandomBytesNeeded)
		keys += scala.util.Random.nextInt.toByte

	// Set up the threading system
	//
	private val root = ConfigFactory.load()
	val actconf = root.getConfig("LocalSystem")
	val context = ActorSystem("localsystem", actconf)
	val bookkeeper = context.actorOf(Props(new Bookkeeper), name = "Bookkeeper")

	var nNeededMessages = config.nrOfMessages
	if (config.nScenario == 2)
		nNeededMessages *= 2

	val master = context.actorOf(Props(new Master(store, config.nWorkers, nNeededMessages, bookkeeper)), name = "Master")
	val workerPool1 = context.actorOf(Props(new Worker(keys, values, store)).withRouter(RoundRobinPool(config.nWorkers)), name = "workerPool1")
	val workerPool2 = context.actorOf(Props(new Worker(keys, values, store)).withRouter(RoundRobinPool(config.nWorkers)), name = "workerPool2")
	val nStatusEveryMs = config.nStatusEverySec * 1000;

	def Run = {
		println("start Run")
		master ! Simulate
		context.awaitTermination()
		store.Shutdown()
		println("end Run")
	}

	///////////////////////////////////////////////////////////////////////////

	class Data(key: Key, v: Value) extends IStorage {
		var value = v

		def Key = key

		def Value = value

		def Construct(k: Key, v: Value) = {
			value = v
		}
	}

	class Worker(keys: Key, values: Value, store: DataStore) extends Actor {
		var nReadExceptions = 0;
		var nWriteExceptions = 0;

		def GetKey(nIndex: Int, nLength: Int): Key = {
			var key = new Key
			for (i <- 0 to nLength)
				key += keys(nIndex + i)
			return key
			// return keys.slice(nIndex, nLength).asInstanceOf[Key]
		}

		def GetValue(nIndex: Int, nLength: Int): Value = {
			var value = new Value
			for (i <- 0 to nLength)
				value += values(nIndex + i)
			return value
			// return values.slice(nIndex, nLength).asInstanceOf[Value]
		}

		def receive = {
			case Add(nIndex: Int, nLength: Int) => {
				try {
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if (config.bDoStorage) store.add(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
						println("Add: Caught exception " + e.getMessage())
					}
				}
				sender ! Result()
			}
			case Put(nIndex: Int, nLength: Int) => {
				try {
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if (config.bDoStorage) store.put(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
						println("Put: Caught exception " + e.getMessage())
					}
				}
				sender ! Result()
			}
			case Del(nIndex: Int) => {
				try {
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, 0))
					if (config.bDoStorage) store.del(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_D(0, start, end)
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
						println("Del: Caught exception " + e.getMessage())
					}
				}
				sender ! Result()
			}
			case Get(nIndex: Int) => {
				try {
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, 0))
					if (config.bDoStorage) store.get(GetKey(nIndex, 8), o)
					val end: Long = System.currentTimeMillis
					sender ! Result_R(o.Value.length, start, end)
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
						println("Get: Caught exception " + e.getMessage())
					}
				}
				sender ! Result()
			}

			// Work for scenario 1
			//
			case S1_put(nIndex: Int, nLength: Int) => {
				try {
					val start: Long = System.currentTimeMillis
					val o = new Data(GetKey(nIndex, 8), GetValue(nIndex, nLength))
					if (config.bDoStorage) store.put(o)
					val end: Long = System.currentTimeMillis
					sender ! Result_W(nLength, start, end)
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
						println("S1_put: Caught exception " + e.getMessage())
					}
				}

				context.system.scheduler.scheduleOnce(Duration(config.nMsgDelayMSec, MILLISECONDS)) {
					workerPool2 ! Get_Put(nIndex, nLength)
				}(scala.concurrent.ExecutionContext.global);

			}
			case Get_Put(nIndex: Int, nLength: Int) => {
				try {
					val key = GetKey(nIndex, 8)

					try {
						// Do reading
						val start1: Long = System.currentTimeMillis
						val i = new Data(key, GetValue(nIndex, 0))
						if (config.bDoStorage) store.get(key, i)
						val end1: Long = System.currentTimeMillis
						sender ! Result_R(i.Value.length, start1, end1)
					}
					catch {
						case e: Exception => {
							if (nReadExceptions < 2) {
								val stackTrace = StackTrace.ThrowableTraceString(e)
								LOG.debug("StackTrace:" + stackTrace)
								println("Get_Put@read: Caught exception " + e.getMessage())
							}
							nReadExceptions += 1
						}
							throw e;
					}

					try {
						// Do writing
						val start2: Long = System.currentTimeMillis
						val o = new Data(key, GetValue(nIndex, nLength))
						if (config.bDoStorage) store.put(o)
						val end2: Long = System.currentTimeMillis
						sender ! Result_W(nLength, start2, end2)
					}
					catch {
						case e: Exception => {
							if (nWriteExceptions < 2) {
								val stackTrace = StackTrace.ThrowableTraceString(e)
								LOG.debug("StackTrace:" + stackTrace)
								println("Get_Put@write: Caught exception " + e.getMessage())
							}
							nWriteExceptions += 1
						}
							throw e;
					}
				}
				catch {
					case e: Exception => {
						val stackTrace = StackTrace.ThrowableTraceString(e)
						LOG.debug("StackTrace:" + stackTrace)
					}
				}
				finally {
					sender ! Result()
				}
			}

			// Send those the messages back to the originator
			//
			case Result() => {
				master ! Result()
			}
			case Result_W(nLength: Int, nStart: Long, nEnd: Long) => {
				master ! Result_W(nLength, nStart, nEnd)
			}
			case Result_R(nLength: Int, nStart: Long, nEnd: Long) => {
				master ! Result_R(nLength, nStart, nEnd)
			}
			case Result_D(nLength: Int, nStart: Long, nEnd: Long) => {
				master ! Result_D(nLength, nStart, nEnd)
			}
			case _ => {
				println("Worker::Catch-All\n")
			}
		}
	}

	class Bookkeeper extends Actor {
		def receive = {
			case Done(nOps: Long, nDurationMs: Long, nWroteBytes: Long, nReadBytes: Long, nDeletesOps: Long) => {
				context.system.shutdown()
				println("\nBookkeeper::Done")
				println("ops = " + nOps)
				println("t elapse = " + Duration.create(nDurationMs, MILLISECONDS).toString())
				println("wrote = " + nWroteBytes)
				println("read = " + nReadBytes)
				println("delete = " + nDeletesOps)
			}
			case PreparationDone(nOps: Long, nDurationMs: Long, nWroteBytes: Long, nReadBytes: Long, nDeletesOps: Long) => {
				println("\nBookkeeper::PreparationDone")
				println("ops = " + nOps)
				println("t elapse = " + Duration.create(nDurationMs, MILLISECONDS).toString())
				println("wrote = " + nWroteBytes)
				println("read = " + nReadBytes)
				println("delete = " + nDeletesOps)
			}
			case _ => {
				println("Bookkeeper::Catch-All\n")
			}
		}
	}

	class Master(store: DataStore, nrOfWorkers: Int, nrOfMessages: Int, bookkeeper: ActorRef) extends Actor {
		var taskStart: Long = System.currentTimeMillis

		var sendMessages: Int = 0
		var recvMessages: Int = 0

		var recvMessagesW: Int = 0
		var nDurationW: Long = 0
		var nSizeW: Long = 0

		var recvMessagesR: Int = 0
		var nDurationR: Long = 0
		var nSizeR: Long = 0

		var recvMessagesD: Int = 0
		var nDurationD: Long = 0
		var nSizeD: Long = 0

		var nLastStatusPrint: Long = System.currentTimeMillis
		var nLastStatusSent: Long = System.currentTimeMillis
		var nLastMessages: Long = 0;
		var nLastSizeW: Long = 0;
		var nLastSizeR: Long = 0;

		def ResetStats = {
			taskStart = System.currentTimeMillis

			recvMessagesW = 0
			nDurationW = 0
			nSizeW = 0

			recvMessagesR = 0
			nDurationR = 0
			nSizeR = 0

			recvMessagesD = 0
			nDurationD = 0
			nSizeD = 0
		}

		def Ops(): Long = {
			return recvMessagesW + recvMessagesR + recvMessagesD
		}

		def Throughput(): String = {
			val taskCurrentTs = System.currentTimeMillis
			return "%4.1f".format(Ops / ((taskCurrentTs - taskStart) / 1000.0))
		}

		def StartNew() = {
			val nLength = config.nMinMessage + ((config.nMaxMessage - config.nMinMessage) * scala.util.Random.nextFloat()).toInt

			if (config.nScenario == 2) {
				if (sendMessages < nrOfMessages / 2) {
					workerPool1 ! Put(sendMessages, nLength)
				}
				else {
					val n = sendMessages % 5

					if (n == 0 || n == 1 || n == 2)
						workerPool1 ! Get(sendMessages - nrOfMessages / 2)
					else if (n == 3)
						workerPool1 ! Put(sendMessages, nLength)
					else
						workerPool1 ! Get_Put(sendMessages - nrOfMessages / 2, nLength)
				}
			}
			else if (config.nScenario == 1) {
				workerPool1 ! S1_put(sendMessages, nLength)
			}
			else {
				workerPool1 ! Put(sendMessages, nLength)
			}

			sendMessages += 1
		}

		def receive = {
			case Simulate => {
				for (i <- 0 until config.nrOfMessagesInTheSystem)
					StartNew()
			}
			case Result() => {
				val taskCurrentTs = System.currentTimeMillis
				recvMessages += 1

				// Print local status
				//

				if ((nLastStatusPrint + nStatusEveryMs) < System.currentTimeMillis) {
					nLastStatusPrint = taskCurrentTs
					val nDurationMs = taskCurrentTs - taskStart
					println("%3.1f".format((recvMessages * 100.0) / nrOfMessages) + "% " + Duration.create(nDurationMs / 1000.0, SECONDS).toString() + " ops: " + Ops() + " thoughput: " + Throughput())
				}

				// Send messages to master with a sample rate of 1/10th
				if ((nLastStatusSent + (nStatusEveryMs / 10)) < taskCurrentTs) {
					nLastStatusSent = taskCurrentTs
					if (externalBookKeeper != null) {
						externalBookKeeper ! SoFar((recvMessagesW + recvMessagesR) - nLastMessages, taskCurrentTs - taskStart, nSizeW - nLastSizeW, nSizeR - nLastSizeR, 0)
						nLastMessages = recvMessagesW + recvMessagesR
						nLastSizeW = nSizeW
						nLastSizeR = nSizeR
					}
				}

				if (config.nScenario == 2 && recvMessages == nrOfMessages / 2) {
					bookkeeper ! PreparationDone((recvMessagesW + recvMessagesR) - nLastMessages, taskCurrentTs - taskStart, nSizeW - nLastSizeW, nSizeR - nLastSizeR, 0)

					if (externalBookKeeper != null)
						externalBookKeeper ! PreparationDone((recvMessagesW + recvMessagesR) - nLastMessages, taskCurrentTs - taskStart, nSizeW - nLastSizeW, nSizeR - nLastSizeR, 0)

					ResetStats
				}

				if (recvMessages == nrOfMessages) {
					// Send the result to the listener
					bookkeeper ! Done(recvMessagesW + recvMessagesR, taskCurrentTs - taskStart, nSizeW, nSizeR, 0)

					if (externalBookKeeper != null)
						externalBookKeeper ! Done(recvMessagesW + recvMessagesR, taskCurrentTs - taskStart, nSizeW, nSizeR, 0)

					// Stops this actor and all its supervised children
					context.stop(self)
				}
				else if (nrOfMessages > sendMessages)
					StartNew()
			}
			case Result_W(nLength: Int, nStart: Long, nEnd: Long) => {
				nDurationW += (nEnd - nStart)
				nSizeW += nLength
				recvMessagesW += 1
			}
			case Result_R(nLength: Int, nStart: Long, nEnd: Long) => {
				nDurationR += (nEnd - nStart)
				nSizeR += nLength
				recvMessagesR += 1
			}
			case Result_D(nLength: Int, nStart: Long, nEnd: Long) => {
				nDurationD += (nEnd - nStart)
				nSizeD += nLength
				recvMessagesD += 1
			}
			case _ => {
				println("Master::Catch-All\n")
			}

		}
	}

}