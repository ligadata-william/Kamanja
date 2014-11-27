
import scala.actors.threadpool.{ Executors, TimeUnit }
import scala.collection.mutable.ArrayBuffer

import java.util.Properties
import kafka.message._
import kafka.producer.{ ProducerConfig, Producer, KeyedMessage, Partitioner }
import java.io.{ InputStream, ByteArrayInputStream }
import java.util.zip.GZIPInputStream
import java.nio.file.{ Paths, Files }
import kafka.utils.VerifiableProperties
import com.ligadata.Utils.KeyHasher
import java.util.Calendar

object ProducerSimpleStats {

  private[this] val _lock = new Object()
  private[this] val _statusInfo = scala.collection.mutable.Map[Int, String]()

  def setKey(key: Int, value: String): Unit = _lock.synchronized {
    _statusInfo(key) = value
  }

  def getKey(key: Int): String = _lock.synchronized {
    _statusInfo.getOrElse(key, "")
  }

  def getDispString(delim: String = ","): String = _lock.synchronized {
    _statusInfo.map(s => s._2).mkString(delim)
  }
}

class ExtractKey {
  def get(inputData: String, partitionkeyidxs: Array[Int], defaultKey: String, keyPartDelim: String): String = {
    if (partitionkeyidxs.size == 0) {
      return defaultKey
    }
    val str_arr = inputData.split(",", -1)
    if (str_arr.size == 0)
      throw new Exception("Not found any values in message")
    return partitionkeyidxs.map(idx => str_arr(idx)).mkString(keyPartDelim)
  }
}

class CustPartitioner(props: VerifiableProperties) extends Partitioner {
  def partition(key: Any, a_numPartitions: Int): Int = {

    if (key == null) return 0
    try {
      if (key.isInstanceOf[Array[Byte]]) {
        val hashCode = SimpleKafkaProducer.keyHasher.hashKey(key.asInstanceOf[Array[Byte]]).abs
        val bucket = (hashCode % a_numPartitions).toInt
        // println("Key : %s, hashCode: %d, Partitions: %d, Bucket : %d".format(new String(key.asInstanceOf[Array[Byte]]), hashCode, a_numPartitions, bucket))
        return (hashCode % a_numPartitions).toInt
      } else {
        // println("Bucket : is always 0")
        return 0
      }
    } catch {
      case e: Exception =>
        {
        }
        return 0
    }
  }
}

object SimpleKafkaProducer {

  class Stats {
    var totalLines: Long = 0;
    var totalRead: Long = 0;
    var totalSent: Long = 0
  }

  val keyHasher = KeyHasher.byName("fnv1a-32")
  val clientId: String = "Client1"

  val compress: Boolean = false
  val synchronously: Boolean = false
  val batchSize: Integer = 1024
  val queueTime: Integer = 50
  val queueSize: Integer = 16 * 1024 * 1024
  val bufferMemory: Integer = 64 * 1024 * 1024
  val messageSendMaxRetries: Integer = 3
  val requestRequiredAcks: Integer = 1

  val codec = if (compress) DefaultCompressionCodec.codec else NoCompressionCodec.codec

  def send(producer: Producer[AnyRef, AnyRef], topic: String, message: String, partIdx: String): Unit = send(producer, topic, message.getBytes("UTF8"), partIdx.getBytes("UTF8"))

  def send(producer: Producer[AnyRef, AnyRef], topic: String, message: Array[Byte], partIdx: Array[Byte]): Unit = {
    try {
      producer.send(new KeyedMessage(topic, partIdx, message))
      //producer.send(new KeyedMessage(topic, message))
    } catch {
      case e: Exception =>
        e.printStackTrace
        sys.exit(1)
    }
  }

  def ProcessGZipFile(producer: Producer[AnyRef, AnyRef], topics: Array[String], threadId: Int, sFileName: String, msg: String, sleeptm: Int, partitionkeyidxs: Array[Int], st: Stats, ignorelines: Int, topicpartitions: Int): Unit = {
    val bis: InputStream = new ByteArrayInputStream(Files.readAllBytes(Paths.get(sFileName)))
    var len = 0
    var readlen = 0
    var totalLen: Int = 0
    var locallinecntr: Int = 0
    val maxlen = 1024 * 1024
    val buffer = new Array[Byte](maxlen)
    val gzis = new GZIPInputStream(bis)
    var tm = System.nanoTime
    val extractKey: ExtractKey = new ExtractKey
    var ignoredlines = 0
    var curTime = System.currentTimeMillis
    do {
      readlen = gzis.read(buffer, len, maxlen - 1 - len)
      if (readlen > 0) {
        totalLen += readlen
        len += readlen
        var startidx: Int = 0
        var isrn: Boolean = false
        for (idx <- 0 until len) {
          if ((isrn == false && buffer(idx) == '\n') || (buffer(idx) == '\r' && idx + 1 < len && buffer(idx + 1) == '\n')) {
            if ((locallinecntr % 20) == 0)
              curTime = System.currentTimeMillis
            locallinecntr += 1
            var strlen = idx - startidx
            if (sleeptm > 0)
              Thread.sleep(sleeptm)
            if (ignoredlines < ignorelines) {
              ignoredlines += 1
            } else {
              if (strlen > 0) {
                val ln = new String(buffer, startidx, idx - startidx)
                val defPartKey = st.totalLines.toString
                val key = extractKey.get(ln, partitionkeyidxs, defPartKey, ".")
                val hashCode = keyHasher.hashKey(key.getBytes()).abs
                val totalParts = (hashCode % (topicpartitions * topics.size)).toInt.abs
                val topicIdx = (totalParts / topicpartitions)
                val partIdx = (totalParts % topicpartitions)
                val sendmsg = if (msg.size == 0) ln else (msg + "," + ln)
                send(producer, topics(topicIdx), sendmsg, partIdx.toString)
                st.totalRead += ln.size
                st.totalSent += sendmsg.size
              }
            }
            startidx = idx + 1;
            if (buffer(idx) == '\r') // Inc one more char in case of \r \n
            {
              startidx += 1;
              isrn = true
            }
            st.totalLines += 1;

            if (st.totalLines % 100 == 0) {
              // ProducerSimpleStats.setKey(threadId, "Tid:%2d, Lines:%8d, Read:%15d, Sent:%15d".format(threadId, st.totalLines, st.totalRead, st.totalSent))
            }

            val curTm = System.nanoTime
            if ((curTm - tm) > 10 * 1000000000L) {
              tm = curTm
              println("Tid:%2d, Time:%10dms, Lines:%8d, Read:%15d, Sent:%15d".format(threadId, curTm / 1000000, st.totalLines, st.totalRead, st.totalSent))
            }
          } else {
            isrn = false
          }
        }

        var destidx: Int = 0
        // move rest of the data left to starting of the buffer
        for (idx <- startidx until len) {
          buffer(destidx) = buffer(idx)
          destidx += 1
        }
        len = destidx
      }
    } while (readlen > 0)

    if (len > 0 && ignoredlines >= ignorelines) {
      val ln = new String(buffer, 0, len)
      val defPartKey = st.totalLines.toString
      val key = extractKey.get(ln, partitionkeyidxs, defPartKey, ".")
      val sendmsg = if (msg.size == 0) ln else (msg + "," + ln)
      send(producer, topics((st.totalLines % topics.size).toInt), sendmsg, key)
      st.totalRead += ln.size
      st.totalSent += sendmsg.size
      st.totalLines += 1;
    }

    val curTm = System.nanoTime
    println("Tid:%2d, Time:%10dms, Lines:%8d, Read:%15d, Sent:%15d, Last, file:%s".format(threadId, curTm / 1000000, st.totalLines, st.totalRead, st.totalSent, sFileName))
    // ProducerSimpleStats.setKey(threadId, "Tid:%2d, Lines:%8d, Read:%15d, Sent:%15d".format(threadId, st.totalLines, st.totalRead, st.totalSent))
    gzis.close();
  }

  def ProcessTextFile(producer: Producer[AnyRef, AnyRef], topics: Array[String], threadId: Int, sFileName: String, msg: String, sleeptm: Int, partitionkeyidxs: Array[Int], st: Stats): Unit = {
    throw new Exception("Not yet handled processing text files")
    /*
    var totalLines: Int = 0

    for (ln <- scala.io.Source.fromFile(sFileName).getLines) {
      if (sleeptm > 0)
        Thread.sleep(sleeptm)
      val sendmsg = if (msg.size == 0) ln else (msg + "," + ln)
      send(producer, topics((st.totalLines % topics.size).toInt), sendmsg, st.totalLines.toString) // totalLines.toString
      st.totalLines += 1;
    }
*/
  }

  def elapsed[A](f: => A): Long = {
    val s = System.nanoTime
    f
    (System.nanoTime - s)
  }

  type OptionMap = Map[Symbol, Any]

  def nextOption(map: OptionMap, list: List[String]): OptionMap = {
    def isSwitch(s: String) = (s(0) == '-')
    list match {
      case Nil => map
      case "--files" :: value :: tail =>
        nextOption(map ++ Map('files -> value), tail)
      case "--sleep" :: value :: tail =>
        nextOption(map ++ Map('sleep -> value), tail)
      case "--gz" :: value :: tail =>
        nextOption(map ++ Map('gz -> value), tail)
      case "--topics" :: value :: tail =>
        nextOption(map ++ Map('topics -> value), tail)
      case "--msg" :: value :: tail =>
        nextOption(map ++ Map('msg -> value), tail)
      case "--threads" :: value :: tail =>
        nextOption(map ++ Map('threads -> value), tail)
      case "--partitionkeyidxs" :: value :: tail =>
        nextOption(map ++ Map('partitionkeyidxs -> value), tail)
      case "--ignorelines" :: value :: tail =>
        nextOption(map ++ Map('ignorelines -> value), tail)
      case "--topicpartitions" :: value :: tail =>
        nextOption(map ++ Map('topicpartitions -> value), tail)
      case "--brokerlist" :: value :: tail =>
        nextOption(map ++ Map('brokerlist -> value), tail)
      case option :: tail => {
        println("Unknown option " + option)
        sys.exit(1)
      }
    }
  }

  def statsPrintFn: Unit = {
    val CurTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new java.util.Date(System.currentTimeMillis))
    println("Stats @" + CurTime + "\n\t" + ProducerSimpleStats.getDispString("\n\t") + "\n")
  }

  def main(args: Array[String]): Unit = {

    val options = nextOption(Map(), args.toList)
    val sFilesNames = options.getOrElse('files, null).asInstanceOf[String]
    if (sFilesNames == null) {
      println("Need input files as parameter")
      sys.exit(1)
    }

    val sAllFls = sFilesNames.replace("\"", "").trim.split(",")
    val sAllTrimFls = sAllFls.map(flnm => flnm.trim)
    val sAllValidTrimFls = sAllTrimFls.filter(flnm => flnm.size > 0)

    val sleeptmstr = options.getOrElse('sleep, "0").toString

    val msg = options.getOrElse('msg, "").toString.replace("\"", "").trim

    val tmptopics = options.getOrElse('topics, "").toString.replace("\"", "").trim.toLowerCase.split(",").map(t => t.trim).filter(t => t.size > 0)

    if (tmptopics.size == 0) {
      println("Need queue(s)")
      sys.exit(1)
    }

    val topics = tmptopics.toList.sorted.toArray // Sort topics by names    

    val brokerlist = options.getOrElse('brokerlist, "").toString.replace("\"", "").trim // .toLowerCase

    if (brokerlist.size == 0) {
      println("Need Brokers list (brokerlist) in the format of HOST:PORT,HOST:PORT")
      sys.exit(1)
    }

    val sleeptm = sleeptmstr.toInt

    val gz = options.getOrElse('gz, "false").toString

    val threads = options.getOrElse('threads, "0").toString.toInt

    if (threads <= 0) {
      println("Threads must be more than 0")
      sys.exit(1)
    }

    val ignorelines = options.getOrElse('ignorelines, "0").toString.toInt

    val topicpartitions = options.getOrElse('topicpartitions, "0").toString.replace("\"", "").toInt

    if (topicpartitions <= 0) {
      println("We should have +ve topicpartitions")
      sys.exit(1)
    }

    val partitionkeyidxs = options.getOrElse('partitionkeyidxs, "").toString.replace("\"", "").trim.split(",").map(part => part.trim).filter(part => part.size > 0).map(part => part.toInt)

    val props = new Properties()
    props.put("compression.codec", codec.toString)
    props.put("producer.type", if (synchronously) "sync" else "async")
    props.put("metadata.broker.list", brokerlist)
    props.put("batch.num.messages", batchSize.toString)
    props.put("batch.size", batchSize.toString)
    props.put("queue.time", queueTime.toString)
    props.put("queue.size", queueSize.toString)
    props.put("message.send.max.retries", messageSendMaxRetries.toString)
    props.put("request.required.acks", requestRequiredAcks.toString)
    props.put("buffer.memory", bufferMemory.toString)
    props.put("buffer.size", bufferMemory.toString)
    props.put("socket.send.buffer", bufferMemory.toString)
    props.put("socket.receive.buffer", bufferMemory.toString)
    props.put("client.id", clientId)
    props.put("partitioner.class", "CustPartitioner");

    val s = System.nanoTime

    if (sAllValidTrimFls.size > 0) {
      var idx = 0

      val flsLists = if (sAllValidTrimFls.size > threads) threads else sAllValidTrimFls.size

      val executor = Executors.newFixedThreadPool(flsLists)
      val FilesForThreads = new Array[ArrayBuffer[String]](flsLists)
      sAllValidTrimFls.foreach(fl => {
        val index = idx % flsLists
        if (FilesForThreads(index) == null)
          FilesForThreads(index) = new ArrayBuffer[String];
        FilesForThreads(index) += fl
        idx = idx + 1
      })

      val statusPrint = new Runnable {
        def run() {
          statsPrintFn
        }
      }

      val scheduledThreadPool = java.util.concurrent.Executors.newScheduledThreadPool(1);

      // scheduledThreadPool.scheduleWithFixedDelay(statusPrint, 0, 10, java.util.concurrent.TimeUnit.SECONDS);

      idx = 0
      FilesForThreads.foreach(fls => {
        if (fls.size > 0) {
          executor.execute(new Runnable() {
            val threadNo = idx
            val flNames = fls.toArray
            override def run() {
              val producer = new Producer[AnyRef, AnyRef](new ProducerConfig(props))
              var tm: Long = 0
              val st: Stats = new Stats
              flNames.foreach(fl => {
                if (gz.trim.compareToIgnoreCase("true") == 0) {
                  tm = tm + elapsed(ProcessGZipFile(producer, topics, threadNo, fl, msg, sleeptm, partitionkeyidxs, st, ignorelines, topicpartitions))
                } else {
                  tm = tm + elapsed(ProcessTextFile(producer, topics, threadNo, fl, msg, sleeptm, partitionkeyidxs, st))
                }
                println("%02d. File:%s ElapsedTime:%.02fms".format(threadNo, fl, tm / 1000000.0))
              })
              producer.close
            }
          })
        }
        idx = idx + 1
      })
      executor.shutdown();
      try {
        executor.awaitTermination(Long.MaxValue, TimeUnit.NANOSECONDS);
      } catch {
        case e: Exception => {}
      }
    }

    // statsPrintFn
    println("Done. ElapsedTime:%.02fms".format((System.nanoTime - s) / 1000000.0))
    sys.exit(0)

  }
}

