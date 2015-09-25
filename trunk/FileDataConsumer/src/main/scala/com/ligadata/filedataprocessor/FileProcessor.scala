package com.ligadata.filedataprocessor


/**
*
 */


import scala.collection.mutable.HashMap
import scala.collection.JavaConverters._
import util.control.Breaks._
import java.io._
import java.nio.file._
import scala.actors.threadpool.{Executors, ExecutorService }

case class BufferLeftoversArea (workerNumber: Int, leftovers: Array[Char], relatedChunk: Long)
case class BufferToChunk(len: Int, payload: Array[Char], chunkNumber: Long)

class FileProcessor(val path:Path, val partitionId: Int) extends Runnable {

  private val watchService = path.getFileSystem().newWatchService()
  private val keys = new HashMap[WatchKey,Path]
  private var kml: KafkaMessageLoader = null

  var isCosuming = true
  var isProducing = true

  private var workerBees: ExecutorService = null

  // QUEUES used in file processing... will be synchronized.
  private var fileQ: scala.collection.mutable.Queue[String] = new scala.collection.mutable.Queue[String]()
  private var msgQ: scala.collection.mutable.Queue[Array[Char]] = scala.collection.mutable.Queue[Array[Char]]()
  private var bufferQ: scala.collection.mutable.Queue[BufferToChunk] = scala.collection.mutable.Queue[BufferToChunk]()
  private var blg = new BufferLeftoversArea(-1, null, -1)

  // Locks used for Q synchronization.
  private var fileQLock = new Object
  private var msgQLock = new Object
  private var bufferQLock = new Object
  private var beeLock = new Object

  private var msgCount = 0

  // Confugurable Properties
  private var dirToWatch: String = ""
  private var message_separator: Char = _
  private var NUMBER_OF_BEES: Int = 2
  private var maxlen: Int = _
  /**
   * Called by the Directory Listener to initialize
   * @param props
   */
  def init(props: scala.collection.mutable.Map[String,String]): Unit = {
    message_separator = props("messageSeparator").toInt.toChar
    dirToWatch = props("dirToWatch")
    NUMBER_OF_BEES = props("workerdegree").toInt
    maxlen = props("workerbuffersize").toInt * 1024 * 1024
    kml = new KafkaMessageLoader (props("kafkaBroker"), props("topic"))
  }

  /**
   *
   * @param file
   * @param code
   */
  private def enQFile(file: String, code: Int): Unit = {
    fileQLock.synchronized {
      fileQ += file
    }
  }

  /**
   *
   * @param code
   * @return
   */
  private def deQFile(code: Int): String = {
    fileQLock.synchronized {
      if (fileQ.isEmpty) {
        return null
      }
      var blah = fileQ.dequeue
      return blah
    }
  }

  private def enQMsg(buffer: Array[Char], bee: Int): Unit = {
    msgQLock.synchronized {
      msgCount += 1
      msgCount = msgCount % 1000
      if (msgCount == 999) println("1000 Messages ENQUEUED ")
     // if (msgCount == 999) println(new String (buffer))
    //  println("Bee " +bee + " MESSAGE Q - ENQ - " +  new String(buffer))
      msgQ += buffer
    }
  }

  private def deQMsg(): Array[Char] = {
    msgQLock.synchronized {
      if (msgQ.isEmpty)  {
        return null
      }
      return msgQ.dequeue
    }
  }

  private def enQBuffer(buffer: BufferToChunk): Unit = {
    bufferQLock.synchronized {
        bufferQ += buffer
    }
  }

  private def deQBuffer(bee: Int): BufferToChunk = {
    msgQLock.synchronized {
      if (bufferQ.isEmpty) {
          return null
      }
      return bufferQ.dequeue
    }
  }

  private def getLeftovers(code: Int): BufferLeftoversArea = {
    beeLock.synchronized {
      return blg
    }
  }

  private def setLeftovers(in: BufferLeftoversArea, code: Int) = {
    beeLock.synchronized {
      blg = in
    }
  }




  /**
   * Register a particular file or directory to be watched
   */
  private def register(dir:Path): Unit = {
    val key = dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.OVERFLOW)
    keys(key) = dir
  }


  /**
   * Each worker bee will run this code... looking for work to do.
   * @param beeNumber
   */
   private def processBuffers(beeNumber: Int) = {
  //private def processBuffer(buffer: Array[Byte], readlen: Int): Unit = {

    var myLeftovers: BufferLeftoversArea = null
    var buffer: BufferToChunk = null;
    // if This is Bee 1... Grab the next buffer from the queue if it is either a first buffer for this file,
    // or the second worker already placed his leftovers.
    while (true) {
      var leftOvers: Array[Char] = new Array[Char](0)
      buffer = deQBuffer(beeNumber)

      // If we are not the first buffer, then pull the leftovers from the leftover area first.  - MOVE THIS TO A LATER
      // POINT TO ENABLE MORE // Processing
      if (buffer != null &&
          buffer.chunkNumber != 0) {
        var foundRelatedLeftovers = false
        while(!foundRelatedLeftovers){
          myLeftovers = getLeftovers(beeNumber)
          if (myLeftovers.relatedChunk == (buffer.chunkNumber - 1)) {
            leftOvers = myLeftovers.leftovers
            foundRelatedLeftovers = true
          } else {
            Thread.sleep(100)
          }
        }
      }

      // OK, we have the leftovers! Now process this buffer...  If we have a buffer and its not an EOF signal
      if (buffer != null) {

        var indx = 0
        var prevIndx = indx
        var temp: String = new String(buffer.payload)
        var totalBuffer = leftOvers ++ buffer.payload
        var totalLen = leftOvers.size + buffer.len

        totalBuffer.foreach(x => {
          if (x.asInstanceOf[Char] == message_separator) {
            var newMsg: Array[Char] = totalBuffer.slice(prevIndx, indx)
            enQMsg(newMsg, beeNumber)
            prevIndx = indx + 1
          }
          indx = indx + 1
        })

        // whatever is left is the leftover we need to pass to another thread.
        indx = scala.math.min(indx, totalLen)
        if (indx != prevIndx) {
          var newFileLeftOvers = BufferLeftoversArea(beeNumber, totalBuffer.slice(prevIndx, indx), buffer.chunkNumber)
          setLeftovers(newFileLeftOvers, beeNumber)
        } else {
          var newFileLeftOvers = BufferLeftoversArea(beeNumber, new Array[Char](0),buffer.chunkNumber)
          setLeftovers(newFileLeftOvers, beeNumber)
        }

      } else {
        // Ok, we did not find a buffer to process on the BufferQ.. wait.
        Thread.sleep(100)
      }
    }
  }

  /**
   * This will be run under a CONSUMER THREAD.
   * @param fileName
   */
  private def readBytesChunksFromFile (fileName: String) : Unit = {
    //val maxlen: Int = 1024 * 1024 * 8
    //val maxlen: Int = 16M
    //val buffer = new Array[Byte](maxlen)
    val buffer = new Array[Char](maxlen)
    var readlen = 0
    var len: Int = 0
    var totalLen = 0
    var chunkNumber = 0

    // Start the worker bees... should only be started the first time..
    if (workerBees == null) {
      workerBees = Executors.newFixedThreadPool(NUMBER_OF_BEES)
      for (i <- 1 to NUMBER_OF_BEES) {
        workerBees.execute(new Runnable() {
          override def run() = {
            processBuffers(i)
          }
        })
      }
    }

    // Grab the InputStream from the file and start processing it.  Enqueue the chunks onto the BufferQ for the
    // worker bees to pick them up.
    //var bis: InputStream = new ByteArrayInputStream(Files.readAllBytes(Paths.get(fileName)))
    var bis = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)))
    do {
       //readlen = bis.read(buffer, len, maxlen-1-len)

      readlen = bis.read(buffer, 0, maxlen -1)
     // println("* READ "+readlen+" bytes from the stream: "+ new String(buffer))
      if (readlen > 0) {
        println("CHUNK # "+ chunkNumber)
        totalLen += readlen
        len += readlen
        var BufferToChunk = new BufferToChunk(readlen, buffer.slice(0,maxlen), chunkNumber)
        enQBuffer(BufferToChunk)
        chunkNumber += 1
      }
    } while (readlen > 0)

    // Pass the leftovers..
    var myLeftovers: BufferLeftoversArea = null
    var foundRelatedLeftovers = false
    while(!foundRelatedLeftovers){
      myLeftovers = getLeftovers(1000)
      // if this is for the last chunk written...
      if (myLeftovers.relatedChunk == (chunkNumber - 1)) {
        println("Found the last chunk of file "+ chunkNumber + " .. " + new  String(myLeftovers.leftovers) + "       "+ myLeftovers.relatedChunk)
        // EnqMsg here, and
        enQMsg(myLeftovers.leftovers, 1000)
        foundRelatedLeftovers = true
      } else {
        Thread.sleep(100)
      }
    }
  }

  /**
   *  This is the "FILE CONSUMER"
   */
  private def doSomeConsuming(): Unit = {
    while (isCosuming) {
      var fileToProcess = deQFile(99)
      var curTimeStart: Long = 0
      var curTimeEnd: Long = 0
      if (fileToProcess == null) {
     //   println("CONSUMER - wake and ..... go back to sleep")
        Thread.sleep(500)
      } else {
     //   println("CONSUMER - WAKE AND BACK .. processing new file " + fileToProcess)
        println("Processing file "+fileToProcess)
        curTimeStart = System.currentTimeMillis
        readBytesChunksFromFile(fileToProcess)
        curTimeEnd = System.currentTimeMillis
        println(" TIME in miliseconds: " + (curTimeEnd - curTimeStart ))
        try {
          var fileStruct = fileToProcess.split("/")
          //(new File(fileToProcess)).renameTo(new File("/tmp/watch/COMPLETE_" + fileStruct(fileStruct.size - 1)))
          println("Renaming file "+ fileToProcess + " to " + fileToProcess + "_COMPLETE")
          (new File(fileToProcess)).renameTo(new File(fileToProcess + "_COMPLETE"))
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

  /**
   * This is a "PUSHER" file.
   */
  private def doSomePushing(): Unit = {
    while (isProducing) {
      var msg = deQMsg
      if (msg == null) {
        Thread.sleep(250)
      }
      else {
        kml.pushData(msg)
      }
    }
  }
  /**
   * The main directory watching thread
   */
  override def run(): Unit = {
    try {
      // Initialize and launch the File Processor thread(s), and kafka producers
      var fileConsumers: ExecutorService = Executors.newFixedThreadPool(2)
      fileConsumers.execute(new Runnable() {
        override def run() = {
          doSomeConsuming
        }
      })

      fileConsumers.execute(new Runnable() {
        override def run() = {
          doSomePushing
        }
      })

      // Register a listener on a watch directory.
      register(path)

      // Process all the existing files in the directory that are not marked complete.
      val d = new File("/tmp/watch")
      if (d.exists && d.isDirectory) {
        val files = d.listFiles.filter(_.isFile).sortWith(_ .lastModified < _.lastModified).toList
        files.foreach(file => {
            if (isValidFile(file.toString)) {
          //    println(file + " last modified " + file.lastModified)
              enQFile(file.toString, 77)
            }
        })
      }
      println("Initialization complete")

      // Begin the listening process, TAKE()
      breakable {
        while (true) {
          println("********        Watcher Running .... ")
          val key = watchService.take()
          val dir = keys.getOrElse(key, null)
          if(dir != null) {
            key.pollEvents.asScala.foreach( event => {
              val kind = event.kind
              println("*** Event: " + kind + " for "+ event.context().asInstanceOf[Path])
              // Only worry about new files.
              if(kind.equals(StandardWatchEventKinds.ENTRY_CREATE)) {
                val event_path = event.context().asInstanceOf[Path]
                val fileName = "/tmp/watch/"+event_path.toString
                if (isValidFile(fileName))
                  enQFile(fileName, 69)
              }
            })
          } else {
            println("WatchKey not recognized!!")
          }

          if (!key.reset()) {
            keys.remove(key)
            if (keys.isEmpty) {
              break
            }
          }
        }
      }
    } catch {
      case ie: InterruptedException => println("InterruptedException: " + ie)
      case ioe: IOException => println("IOException: " + ioe)
      case e: Exception => println("Exception: " + e)
    }
  }

  private def isValidFile(fileName: String): Boolean = {
    if (!fileName.endsWith("_COMPLETE"))
      return true
    return false
  }
}


