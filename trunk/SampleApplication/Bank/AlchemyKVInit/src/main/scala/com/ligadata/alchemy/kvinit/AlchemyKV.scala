package com.ligadata.alchemy.kvinit

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
import com.ligadata.OnLEPBankPoc._
import com.ligadata.OnLEPBase._


trait LogTrait {
    val loggerName = this.getClass.getName()
    val logger = Logger.getLogger(loggerName)
}

object AlchemyKV extends App with LogTrait {

    def usage : String = {
""" 
Usage: scala com.ligadata.alchemy.kvinit.AlchemyKV 
    --kvname <name of kv store> 
    --kvpath <where to put kvname db> 
    --csvpath <input to load> 
    --keyfieldname  <name of one of the fields in the first line of the csvpath file>
    --dump <if any{Y | y | yes | Yes} just dump an existing store>

Nothing fancy here.  Mapdb kv store is created from arguments... style is hash map. Support
for other styles of input (e.g., JSON, XML) are not supported.  This is tied to the 
Alchemy Bank POC.  Clearly something can be done to generalize this by loading the message or
container def jar, doing introspection, dynamic loading and the rest.  This is left for 
another day.

The kvname serves as both the name of the physical hdb to be written to disk and the 
name of the container (e.g., CustomerPreferences_100, AlertParameters_100, AlertHistory_100, TukTier_100).

It is expected that the first row of the csv file will be the column names.  One of the names
must be specified as the key field name.  Failure to find this name causes termination and
no kv store creation.
      
"""
    }
     
    override def main (args : Array[String]) {
        
        logger.debug("AlchemyKV.main begins")


        if (args.length == 0) logger.error(usage)
        val arglist = args.toList
        type OptionMap = Map[Symbol, String]
        logger.debug(arglist)
        def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
          list match {
            case Nil => map
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
            case option :: tail => logger.error("Unknown option " + option) 
                                   sys.exit(1) 
          }
        }
        
        val options = nextOption(Map(),arglist)

        val kvname = if (options.contains('kvname)) options.apply('kvname) else null
        val kvpath = if (options.contains('kvpath)) options.apply('kvpath) else null 
        val csvpath = if (options.contains('csvpath)) options.apply('csvpath) else null 
        val keyfieldname = if (options.contains('keyfieldname)) options.apply('keyfieldname) else null 
        val dump = if (options.contains('dump)) options.apply('dump) else null 
        
        var valid : Boolean = (kvname != null && kvpath != null && csvpath != null && keyfieldname != null) 

        if (valid) {
            val kvmaker : AlchemyKV = new AlchemyKV(kvname
                                                    , kvpath
                                                    , csvpath
                                                    , keyfieldname )

 
            if (dump != null && dump.toLowerCase().startsWith("y")) {
            	val dstore : DataStore = kvmaker. openstore
    			kvmaker.dump(dstore)
            	dstore.Shutdown()
            } else {
            	val dstore : DataStore = kvmaker.build     
            	kvmaker.dump(dstore)
            	dstore.Shutdown()
        	}
        } else {
        	logger.error("Illegal and/or missing arguments")
        	logger.error(usage)
        }
    } 
}

class AlchemyKV(val kvname : String
                , val kvpath : String
                , val csvpath : String
                , val keyfieldname : String
                , val kvstyle : String = "hashmap" ) extends LogTrait {
    
    val csvdata : List[String] = Source.fromFile(csvpath).mkString.split('\n').toList
    val header : Array[String] = csvdata.head.split(',').map(_.trim.toLowerCase)
    val isOk : Boolean = header.contains(keyfieldname.toLowerCase())
    val container : BaseContainer = make(kvname)
    var keyPos : Int = 0

    def openstore : DataStore = {
      
        val tablevalue : String = kvname.split('_').head
        var connectinfo : PropertyMap = new PropertyMap
        connectinfo+= ("connectiontype" -> "hashmap")
        connectinfo+= ("path" -> s"$kvpath")
        connectinfo+= ("schema" -> s"$kvname")
        connectinfo+= ("table" -> s"$tablevalue")
        connectinfo+= ("inmemory" -> "false")
        connectinfo+= ("withtransaction" -> "true")
        
        val kvstore : DataStore = KeyValueManager.Get(connectinfo)
        kvstore
    }
    
    def locateKeyPos : Unit = {
         /** locate the key position */
        keyPos = 0
        breakable {
        	val kvKey : String = keyfieldname.trim.toLowerCase()
        	header.foreach( datum => {
        		if (datum.trim.toLowerCase() == kvKey) {
        			break
        		}
        		keyPos += 1 
        	})
        }
    }
    
    def build : DataStore = {
        if (! isOk) return null

        val kvstore : DataStore = openstore
        kvstore.TruncateStore
        
        locateKeyPos /** locate key idx */
        
        val tx : Transaction = kvstore.beginTx()
        val csvdataRecs : List[String] = csvdata.tail
        csvdataRecs.foreach(tuples => {
            /** if we can make one ... we add the data to the store. This will crash if the data is bad */
            container.populate(new DelimitedData(tuples, ","))

            val data : Array[String] = tuples.split(',').map(_.trim)
            val key : String = data(keyPos)
            SaveObject(key, tuples, kvstore)
        })
        kvstore.endTx(tx)

        kvstore
    }
    
 
    def printTuples (tupleBytes : Value)  {
    	val buffer : StringBuilder = new StringBuilder
    	val tuplesAsString : String = tupleBytes.toString
    	tupleBytes.foreach(c => buffer.append(c.toChar))
    	val tuples : String = buffer.toString
    	//logger.info(tuples)
      
    	container.populate(new DelimitedData(tuples, ","))
    	logger.info(s"\n$container")
    }

    def dump(datastore : DataStore) : Unit = {
    	logger.info(s"\nDump of data store $kvname")
    	
        locateKeyPos /** locate key idx */
        
    	val printOne = (tupleBytes : Value) => { printTuples(tupleBytes) }
    	val csvdataRecs : List[String] = csvdata.tail
        csvdataRecs.foreach(tuples => {
            /** if we can make one ... we add the data to the store. This will crash if the data is bad */

            val data : Array[String] = tuples.split(',').map(_.trim)
            val key : String = data(keyPos)
            
            datastore.get(makeKey(key), printOne)
        })
    }
    
 	private def makeKey(key : String) : com.ligadata.keyvaluestore.Key = {
		var k = new com.ligadata.keyvaluestore.Key
	    for(c <- key ){
	        k += c.toByte
	    }
		k
	}

	private def makeValue(value : String) : com.ligadata.keyvaluestore.Value = {
		var v = new com.ligadata.keyvaluestore.Value
	    for(c <- value ){
	        v += c.toByte
	    }
		v
	}
 
    private def make(containerName : String) : BaseContainer = {
        val container : BaseContainer = containerName match {
            case "CustomerPreferences_100" =>  new CustomerPreferences_100
            case "AlertParameters_100" =>  new AlertParameters_100
            case "AlertHistory_100" => new AlertHistory_100
            case "TukTier_100" => new TukTier_100
            case _ => {
                logger.error(s"Container name ${'"'}$containerName${'"'} supplied in the --kvname argument is unknown... aborting...")
                sys.exit
            }
        }
        container
    }


    private def SaveObject(key: String, value: String, store: DataStore){
        object i extends IStorage{
            var k = new com.ligadata.keyvaluestore.Key
            var v = new com.ligadata.keyvaluestore.Value
            for(c <- key ){
                k += c.toByte
            }
            for(c <- value ){
                v += c.toByte
            }
            def Key = k
            def Value = v
            def Construct(Key: com.ligadata.keyvaluestore.Key, Value: com.ligadata.keyvaluestore.Value) = {}
        }
        store.put(i)
    }


}


