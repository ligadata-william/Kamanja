package com.ligadata.MedEnvContext


import scala.collection.immutable.Map
import scala.collection.mutable._
import scala.util.control.Breaks._
import scala.reflect.runtime.{ universe => ru }
import org.apache.log4j.Logger
import com.ligadata.keyvaluestore._
import com.ligadata.keyvaluestore.mapdb._
import com.ligadata.OnLEPBase._
import com.ligadata.OnLEPBase.{ EnvContext, BaseContainer }
import com.ligadata.olep.metadata._
import java.net.URLClassLoader

trait LogTrait {
    val loggerName = this.getClass.getName()
    val logger = Logger.getLogger(loggerName)
}

/** 
 *  The MedEnvContext supports kv stores that are based upon MapDb hash tables.  These kvstores currently are loaded
 *  into maps (similar to the SimpleEnvContextImpl... different keys used) COMPLETELY at initialization (when initContainers
 *  is called) time.
 */
object MedEnvContext extends EnvContext with LogTrait {
	private[this] val _lock = new Object()
	private[this] var _containers = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, BaseContainer]]()
	private[this] var _messages = scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, BaseMsg]]()
	
	/** Add this one too for caching the arrays that are returned from the loaded map's values */
	private [this] var _filterArrays : scala.collection.mutable.Map[String, Array[BaseContainer]] = scala.collection.mutable.Map[String, Array[BaseContainer]]()

	private[this] var _bInitializedContainers: Boolean = false
	private[this] var _bInitializedMessages: Boolean = false
    
   /** 
    *  Intitialize the container cache from the mapdb items found in the supplied dataPath.  The names of the 
    *  mapdb files are the typename (sans package names) with a .hdb suffix.  Caches (the maps allocated) are 
    *  filled with 
    */
  	val buffer : StringBuilder = new StringBuilder
  	def initContainers(mgr : MdMgr, dataPath : String, containerNames: Array[String]): Unit = _lock.synchronized {
	  	if (_bInitializedContainers) {
    		throw new RuntimeException("Already Initialized")
    	}
    	containerNames.foreach(c => {
    		val names : Array[String] = c.split('.')
    		val namespace : String = names.head
    		val name : String = names.last
			val containerType : ContainerDef = mgr.ActiveContainer(namespace,name)
    		if (containerType != null) {
    		
				val typeName : String = containerType.typeString.split('.').last
				val tableName : String = typeName.split('_').head
				
    			/** create a map to cache the entries to be resurrected from the mapdb */
				_containers(tableName.toLowerCase()) = scala.collection.mutable.Map[String, BaseContainer]()

				/** 
				 *  Note: Regardless of the container, an attempt is made to open a datastore.  Should
				 *  it be empty, we don't don't bother to load.  There are several containerTypes that 
				 *  don't have content (the EnvContext is prime example).  
				 *  
				 */
				val dstore : DataStore = openstore(typeName, tableName, dataPath) 
				val keyCollector = (key : Key) => { collectKey(key) }
				keys.clear
				dstore.getAllKeys( keyCollector )
				if (keys.size > 0) {
					loadMap(containerType, keys, dstore, _containers(tableName.toLowerCase()) )
				}
			
    		}
    	})
    	
    	_bInitializedContainers = true
    }
  
    
   /** 
    *  Intitialize the messages cache
    */
  	def initMessages(mgr : MdMgr, dataPath : String, msgNames: Array[String]): Unit = _lock.synchronized {
	  	if (_bInitializedMessages) {
    		throw new RuntimeException("Already Initialized")
    	}
    	msgNames.foreach(c => {
    		val names : Array[String] = c.split('.')
    		val namespace : String = names.head
    		val name : String = names.last
			val msgType : MessageDef = mgr.ActiveMessage(namespace,name)
    		if (msgType != null) {
				_messages(msgType.FullName.toLowerCase) = scala.collection.mutable.Map[String, BaseMsg]()
    		}
    	})
    	
    	_bInitializedMessages = true
    }

  	/**
  	 *  For the current container, load the values for each key, coercing it to the appropriate BaseContainer, and storing
  	 *  each in the supplied map.  
  	 *  
  	 *  @param containerType - a ContainerDef that describes the current container.  Its typeName is used to create an instance
  	 *  	of the BaseContainer derivative.
  	 *  @param keys : the keys to use on the dstore to extract a given element.
  	 *  @param dstore : the mapdb handle
  	 *  @param map : the map to be updated with key/BaseContainer pairs.
  	 */
  	def loadMap(containerType : ContainerDef
  				, keys : ArrayBuffer[String]
				, dstore : DataStore
				, map: scala.collection.mutable.Map[String, BaseContainer]) : Unit = {
  	  
  		var container : BaseContainer = null
		keys.foreach(key => {
			try {
		  		val buildOne = (tupleBytes : Value) => { buildContainer(tupleBytes, container) }
				container = Class.forName(containerType.typeString).newInstance().asInstanceOf[BaseContainer] 
		  		dstore.get(makeKey(key), buildOne)
		  		map(key.toLowerCase) = container
			} catch {
			  case e: ClassNotFoundException => {
				  logger.error(s"unable to create a container named ${containerType.typeString}... is it defined in the metadata?")
				  e.printStackTrace()
				  throw e
			  }
			  case ooh : Throwable => {
				  logger.error(s"unknown error encountered while processing ${containerType.typeString}.. stack trace = ${ooh.printStackTrace}")
				  throw ooh
			  }
			}
		})
    	logger.trace("Loaded %d objects".format(map.size))    	
  	}

    def buildContainer (tupleBytes : Value, container : BaseContainer)  {
    	buffer.clear
    	tupleBytes.foreach(c => buffer.append(c.toChar))
    	val tuples : String = buffer.toString
    	//logger.trace(tuples)
    	
    	container.populate(new DelimitedData(tuples, ","))
    	// logger.trace(s"\n$container")    	
    }

  	var keys : ArrayBuffer[String] = ArrayBuffer[String]()

  	def collectKey(key : Key) : Unit = {
    	buffer.clear
    	key.foreach(c => buffer.append(c.toChar))
    	val containerKey : String = buffer.toString
    	keys += containerKey
  	}
	
	def openstore(typeName : String, tableName : String, dataPath : String) : DataStore = {
		var connectinfo : PropertyMap = new PropertyMap
		connectinfo+= ("connectiontype" -> "hashmap")
		connectinfo+= ("path" -> s"$dataPath/kvstores")
		connectinfo+= ("schema" -> s"$typeName")
		connectinfo+= ("table" -> s"$tableName")
		connectinfo+= ("inmemory" -> "false")
		connectinfo+= ("withtransaction" -> "false")
	    
	    val kvstore : DataStore = KeyValueManager.Get(connectinfo)
	    kvstore
	}

	override def getObjects(containerName: String, key: String): Array[BaseContainer] = _lock.synchronized {
	    // bugbug: implement partial match
	    //Array(getObject(containerName, key))
	    	    
	    /**  
	    	Check the "FilterArrays" map for the array with the name "key.toLowerCase" to be returned.  If not present, 
	    	access the _containers map with the key. Project the keys of the map tuples to an array.  
	    	Add it to the FilterArrays map and return the array as the function result.
	    */
	    val lKey : String = key.toLowerCase()
	    val filterSetValues : Array[BaseContainer] = if (_filterArrays.contains(lKey)) {
	    	_filterArrays.apply(lKey)
	    } else {
	    	val setVals : Array[BaseContainer] = if (_containers.contains(lKey)) {
		    	val map : scala.collection.mutable.Map[String, BaseContainer] = _containers(lKey)
		    	val filterVals : Array[BaseContainer] = map.values.toArray
		    	_filterArrays(lKey) = filterVals /** cache it for subsequent calls */
		    	filterVals
	    	} else {
	    		Array[BaseContainer]()
	    	}
	    	setVals
	    }
	    filterSetValues	    
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
 
 	override def getObject(containerName: String, key: String): BaseContainer = _lock.synchronized {
 		val container = _containers.getOrElse(containerName.toLowerCase(), null)
		if (container != null) {
 			val v = container.getOrElse(key.toLowerCase(), null)
 			// LOG.info("Found Container:" + containerName + ". Value:" + v)
 			v
 		} else null
 	}

 	override def setObject(containerName: String, key: String, value: BaseContainer): Unit = _lock.synchronized {
 		val container = _containers.getOrElse(containerName.toLowerCase(), null)
		if (container != null) container(key.toLowerCase()) = value
		// bugbug: throw exception
 	}

 	override def setObject(containerName: String, elementkey: Any, value: BaseContainer): Unit = _lock.synchronized {
 		val container = _containers.getOrElse(containerName.toLowerCase(), null)
		if (container != null) {
			val key : String = elementkey.toString.toLowerCase()
			container(key) = value
			logger.info(s"Replacing container '$containerName' entry for key '$key' ... value = \n${value.toString}")
			//writeThru
		}
		// bugbug: throw exception
		
 	}
 	
  override def getMsgObject(msgName: String, key: String): BaseMsg = {
 		val msg = _messages.getOrElse(msgName.toLowerCase, null)
		if (msg != null) {
 			val v = msg.getOrElse(key.toLowerCase(), null)
 			// LOG.info("Found Message:" + msgName + ". Value:" + v)
 			v
 		} else null
  }
  
  override def setMsgObject(msgName: String, key: String, value: BaseMsg): Unit = {
 		val msg = _messages.getOrElse(msgName.toLowerCase, null)
		if (msg != null) msg(key.toLowerCase) = value
		// bugbug: throw exception
  }
 	
 	private def writeThru(key : String, value : String) {
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
        //store.put(i) 
 	  
 	}
 	
}


