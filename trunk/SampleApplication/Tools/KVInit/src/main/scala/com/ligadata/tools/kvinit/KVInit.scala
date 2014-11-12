package com.ligadata.tools.kvinit

import scala.collection.mutable._
import scala.io.Source
import scala.util.control.Breaks._
import java.io.BufferedWriter
import java.io.FileWriter
import sys.process._
import java.io.PrintWriter
import java.io.File
import java.io.FileInputStream
import java.io.InputStream
import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.OnLEPBase._
import com.ligadata.edifecs._

trait LogTrait {
  val loggerName = this.getClass.getName()
  val logger = Logger.getLogger(loggerName)
}

object KVInit extends App with LogTrait {

  def usage: String = {
    """ 
Usage: scala com.ligadata.kvinit.KVInit 
    --kvname <full package qualified name of a Container or Message> 
    --classpath <full package qualified class name of a BaseContainer or BaseMsg derivative>
    --kvpath <where to put kvname db> 
    --csvpath <input to load> 
    --keyfieldname  <name of one of the fields in the first line of the csvpath file>
    --dump <if any{Y | y | yes | Yes} just dump an existing store>

Nothing fancy here.  Mapdb kv store is created from arguments... style is hash map. Support
for other styles of input (e.g., JSON, XML) are not supported.  
      
The name of the kvstore will be the classname(without it path).

It is expected that the first row of the csv file will be the column names.  One of the names
must be specified as the key field name.  Failure to find this name causes termination and
no kv store creation.
      
Sample uses:
      java -jar /tmp/OnLEPInstall/KVInit-1.0 --kvname --classname com.ligadata.edifecs.SputumCodes_100        --kvpath /tmp/OnLEPInstall/kvstores --csvpath /tmp/OnLEPInstall/sampledata/sputumCodes.csv       --keyfieldname icd9Code       
      
"""
  }

  override def main(args: Array[String]) {

    logger.debug("KVInit.main begins")

    if (args.length == 0) logger.error(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, String]
    logger.debug(arglist)
    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "--classname" :: value :: tail =>
          nextOption(map ++ Map('classname -> value), tail)
        case "--kvname" :: value :: tail =>
          nextOption(map ++ Map('kvname -> value), tail)
        case "--kvpath" :: value :: tail =>
          nextOption(map ++ Map('kvpath -> value), tail)
        case "--csvpath" :: value :: tail =>
          nextOption(map ++ Map('csvpath -> value), tail)
        case "--keyfieldname" :: value :: tail =>
          nextOption(map ++ Map('keyfieldname -> value), tail)
        case "--dump" :: value :: tail =>
          nextOption(map ++ Map('dump -> value), tail)
        case option :: tail =>
          logger.error("Unknown option " + option)
          sys.exit(1)
      }
    }

    val options = nextOption(Map(), arglist)

    val classname = if (options.contains('classname)) options.apply('classname) else null
    val kvname = if (options.contains('kvname)) options.apply('kvname) else null
    val kvpath = if (options.contains('kvpath)) options.apply('kvpath) else null
    val csvpath = if (options.contains('csvpath)) options.apply('csvpath) else null
    val keyfieldname = if (options.contains('keyfieldname)) options.apply('keyfieldname) else null
    val dump = if (options.contains('dump)) options.apply('dump) else null

    var valid: Boolean = (classname != null && kvpath != null && csvpath != null && keyfieldname != null && kvname != null)

    if (valid) {
      val kvmaker: KVInit = new KVInit(classname, kvname.toLowerCase, kvpath, csvpath, keyfieldname)

      if (dump != null && dump.toLowerCase().startsWith("y")) {
        val dstore: DataStore = kvmaker.openstore
        kvmaker.dump(dstore)
        dstore.Shutdown()
      } else {
        val dstore: DataStore = kvmaker.build
        //kvmaker.dump(dstore)
        dstore.Shutdown()
      }
    } else {
      logger.error("Illegal and/or missing arguments")
      logger.error(usage)
    }
  }
}

class KVInit(val classname: String, val kvname: String, val kvpath: String, val csvpath: String, val keyfieldname: String, val kvstyle: String = "hashmap") extends LogTrait {

  val csvdata: List[String] = Source.fromFile(csvpath).mkString.split("\n", -1).toList
  val header: Array[String] = csvdata.head.split(",", -1).map(_.trim.toLowerCase)
  val isOk: Boolean = header.contains(keyfieldname.toLowerCase())
  var keyPos: Int = 0

  def openstore: DataStore = {

    var connectinfo: PropertyMap = new PropertyMap
    connectinfo += ("connectiontype" -> "hashmap")
    connectinfo += ("path" -> s"$kvpath")
    connectinfo += ("schema" -> s"$kvname")
    connectinfo += ("table" -> s"$kvname")
    connectinfo += ("inmemory" -> "false")
    connectinfo += ("withtransaction" -> "false")

    val kvstore: DataStore = KeyValueManager.Get(connectinfo)
    kvstore
  }

  def locateKeyPos: Unit = {
    /** locate the key position */
    keyPos = 0
    breakable {
      val kvKey: String = keyfieldname.trim.toLowerCase()
      header.foreach(datum => {
        if (datum.trim.toLowerCase() == kvKey) {
          break
        }
        keyPos += 1
      })
    }
  }

  def build: DataStore = {
    buildContainerOrMessage
  }

  def buildContainerOrMessage: DataStore = {
    if (!isOk) return null

    val kvstore: DataStore = openstore
    kvstore.TruncateStore

    locateKeyPos
    /** locate key idx */

    val csvdataRecs: List[String] = csvdata.tail
    csvdataRecs.foreach(tuples => {
      if (tuples.size > 0) {
        /** if we can make one ... we add the data to the store. This will crash if the data is bad */
        val inputData = new DelimitedData(tuples, ",")
        inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
        inputData.curPos = 0

        val key: String = inputData.tokens(keyPos)
        SaveObject(key, tuples, kvstore, "CSV")
      }
    })

    kvstore
  }

  def printTuples(tupleBytes: Value) {
    val buffer: StringBuilder = new StringBuilder
    val tuplesAsString: String = tupleBytes.toString
    tupleBytes.foreach(c => buffer.append(c.toChar))
    val tuples: String = buffer.toString
    //logger.info(tuples)

    val inputData = new DelimitedData(tuples, ",")
    inputData.tokens = inputData.dataInput.split(inputData.dataDelim, -1)
    inputData.curPos = 0
    logger.info(s"\n$kvname")
  }

  def dump(datastore: DataStore): Unit = {
    logger.info(s"\nDump of data store $kvname")

    locateKeyPos
    /** locate key idx */

    val printOne = (tupleBytes: Value) => { printTuples(tupleBytes) }
    val csvdataRecs: List[String] = csvdata.tail
    csvdataRecs.foreach(tuples => {
      /** if we can make one ... we add the data to the store. This will crash if the data is bad */

      val data: Array[String] = tuples.split(",", -1).map(_.trim)
      val key: String = data(keyPos)

      datastore.get(makeKey(key), printOne)
    })
  }

  private def makeKey(key: String): com.ligadata.keyvaluestore.Key = {
    var k = new com.ligadata.keyvaluestore.Key
    k ++= key.toLowerCase.getBytes("UTF8")

    k
  }

  private def makeValue(value: String, serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value.getBytes("UTF8")

    v
  }

  private def makeValue(value: Array[Byte], serializerInfo: String): com.ligadata.keyvaluestore.Value = {
    var v = new com.ligadata.keyvaluestore.Value
    v ++= serializerInfo.getBytes("UTF8")

    // Making sure we write first 32 bytes as serializerInfo. Pad it if it is less than 32 bytes
    if (v.size < 32) {
      val spacebyte = ' '.toByte
      for (c <- v.size to 32)
        v += spacebyte
    }

    // Trim if it is more than 32 bytes
    if (v.size > 32) {
      v.reduceToSize(32)
    }

    // Saving Value
    v ++= value

    v
  }

  private def SaveObject(key: String, value: String, store: DataStore, serializerInfo: String) {
    object i extends IStorage {
      var k = makeKey(key)
      var v = makeValue(value, serializerInfo)

      def Key = k
      def Value = v
      def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
    }
    store.put(i)
  }

}


