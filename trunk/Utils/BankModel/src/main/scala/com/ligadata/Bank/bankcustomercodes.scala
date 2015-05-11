
//package com.ligadata.messagescontainers
package com.ligadata.Bank
    
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats
import org.json4s.Formats
import scala.xml.XML
import scala.xml.Elem
import com.ligadata.FatafatBase.{InputData, DelimitedData, JsonData, XmlData}
import com.ligadata.BaseTypes._
import com.ligadata.FatafatBase.SerializeDeserialize
import java.io.{ DataInputStream, DataOutputStream , ByteArrayOutputStream}

import com.ligadata.FatafatBase.{BaseMsg, BaseMsgObj, TransformMessage, BaseContainer, MdBaseResolveInfo, MessageContainerBase}

object System_BankCustomerCodes_1000001_1431110308663 extends BaseMsgObj {
	override def TransformDataAttributes: TransformMessage = null
	override def NeedToTransformData: Boolean = false
	override def FullName: String = "System.BankCustomerCodes"
	override def NameSpace: String = "System"
	override def Name: String = "BankCustomerCodes"
	override def Version: String = "000000.000001.000001"
	override def CreateNewMessage: BaseMsg  = new System_BankCustomerCodes_1000001_1431110308663()
	override def IsFixed:Boolean = false;
	override def IsKv:Boolean = true;

	val partitionKeys : Array[String] = Array("feetype")
    val partKeyPos = Array(0)
        
    override def PartitionKeyData(inputdata:InputData): Array[String] = {
    	if (partKeyPos.size == 0 || partitionKeys.size == 0)
    		return Array[String]()
    	if (inputdata.isInstanceOf[DelimitedData]) {
    		val csvData = inputdata.asInstanceOf[DelimitedData]
    			if (csvData.tokens == null) {
            		return partKeyPos.map(pos => "")
            	}
    		return partKeyPos.map(pos => csvData.tokens(pos))
    	} else if (inputdata.isInstanceOf[JsonData]) {
    		val jsonData = inputdata.asInstanceOf[JsonData]
    		val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
    		if (mapOriginal == null) {
    			return partKeyPos.map(pos => "")
    		}
    		val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    		mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })
    		return partitionKeys.map(key => map.getOrElse(key, "").toString)
    	} else if (inputdata.isInstanceOf[XmlData]) {
    		val xmlData = inputdata.asInstanceOf[XmlData]
                    // Fix this
    	} else throw new Exception("Invalid input data")
    	return Array[String]()
    }
    
	val primaryKeys : Array[String] = Array("feetype", "feeammount")
    val prmryKeyPos = Array(0,1)
	  
    override def PrimaryKeyData(inputdata:InputData): Array[String] = {
    	if (prmryKeyPos.size == 0 || primaryKeys.size == 0)
    		return Array[String]()
    	if (inputdata.isInstanceOf[DelimitedData]) {
    		val csvData = inputdata.asInstanceOf[DelimitedData]
    		if (csvData.tokens == null) {
            	return prmryKeyPos.map(pos => "")
            }
    		return prmryKeyPos.map(pos => csvData.tokens(pos))
    	} else if (inputdata.isInstanceOf[JsonData]) {
    		val jsonData = inputdata.asInstanceOf[JsonData]
    		val mapOriginal = jsonData.cur_json.get.asInstanceOf[Map[String, Any]]
    		if (mapOriginal == null) {
    			return prmryKeyPos.map(pos => "")
    		}
    		val map: scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    		mapOriginal.foreach(kv => { map(kv._1.toLowerCase()) = kv._2 })

    		return primaryKeys.map(key => map.getOrElse(key, "").toString)
    	} else if (inputdata.isInstanceOf[XmlData]) {
    		val xmlData = inputdata.asInstanceOf[XmlData]
                    // Fix this
    	} else throw new Exception("Invalid input data")
    	return Array[String]()
    }
    
}

class System_BankCustomerCodes_1000001_1431110308663 extends BaseMsg {
	override def IsFixed:Boolean = false;
	override def IsKv:Boolean = true;

	override def FullName: String = "System.BankCustomerCodes"
	override def NameSpace: String = "System"
	override def Name: String = "BankCustomerCodes"
	override def Version: String = "000000.000001.000001"

	var feetype:String = _ ;
	var feeammount:Int = _ ;
    var keys = Map(("feetype", 0),("feeammount", 1)) 
 
   var fields: scala.collection.mutable.Map[String, (Int, Any)] = new scala.collection.mutable.HashMap[String, (Int, Any)];
	
	
	val messagesAndContainers = Set("") 
 
	val typs = Array(com.ligadata.BaseTypes.StringImpl,com.ligadata.BaseTypes.IntImpl); 
	 val typsStr = Array("com.ligadata.BaseTypes.StringImpl","com.ligadata.BaseTypes.IntImpl"); 
 val prevVerMatchKeys = Array("") 
 
    private def toStringForKey(key: String): String = {
	  val field = fields.getOrElse(key, (-1, null))
	  if (field._2 == null) return ""
	  field._2.toString
	}
     val prevVerTypesNotMatch = Array("") 
 
    private def getStringIdxFromPrevFldValue(oldTypName: Any, value: Any): (String, Int) = {
	  	var data: String = null    
	  	oldTypName match {
	  		
	  		case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
	    }
	    return (data, typsStr.indexOf(oldTypName))
	  }
    
	override def PartitionKeyData: Array[String] = Array(toStringForKey("feetype"))

	override def PrimaryKeyData: Array[String] = Array(toStringForKey("feetype"), toStringForKey("feeammount"))

    override def set(key: String, value: Any): Unit = {
	  if (key == null) throw new Exception(" key should not be null in set method")
	  fields.put(key, (-1, value))
    }
    override def get(key: String): Any = {
      fields.get(key) match {
    	 case Some(f) => {
    		  //println("Key : "+ key + " Idx " + f._1 +" Value" + f._2 )
    		  return f._2
    	}
    	case None => {
    		  //println("Key - value null : "+ key )
    		  return null
    	  }
    	}
     // fields.get(key).get._2
    }

    override def getOrElse(key: String, default: Any): Any = {
      fields.getOrElse(key, (-1, default))._2
    }
    val collectionTypes = Set("") 
 
    override def AddMessage(childPath: Array[(String, String)], msg: BaseMsg): Unit = { }
     
     
    override def GetMessage(childPath: Array[(String, String)], primaryKey:Array[String]): com.ligadata.FatafatBase.BaseMsg = {
       return null
    } 
     
     
  def populate(inputdata:InputData) = {
	  if (inputdata.isInstanceOf[DelimitedData])	
		populateCSV(inputdata.asInstanceOf[DelimitedData])
	  else if (inputdata.isInstanceOf[JsonData])
			populateJson(inputdata.asInstanceOf[JsonData])
	  else if (inputdata.isInstanceOf[XmlData])
			populateXml(inputdata.asInstanceOf[XmlData])
	  else throw new Exception("Invalid input data")
    
    }
		
  private def populateCSV(inputdata:DelimitedData): Unit = {
	val list = inputdata.tokens
    val arrvaldelim = "~"
	try{
		if(list.size < 2) throw new Exception("Incorrect input data size")
  
 	}catch{
		case e:Exception =>{
			e.printStackTrace()
  			throw e
		}
	}
  }
	  
  private def populateJson(json:JsonData) : Unit = {
	try{
         if (json == null || json.cur_json == null || json.cur_json == None) throw new Exception("Invalid json data")
     	 assignJsonData(json)
	}catch{
	    case e:Exception =>{
   	    	e.printStackTrace()
   	  		throw e	    	
	  	}
	  }
	}
	  
    def CollectionAsArrString(v: Any): Array[String] = {
	  if (v.isInstanceOf[Set[_]]) {
	  	return v.asInstanceOf[Set[String]].toArray
	  }
	  if (v.isInstanceOf[List[_]]) {
	  	return v.asInstanceOf[List[String]].toArray
	  }
	  if (v.isInstanceOf[Array[_]]) {
     	return v.asInstanceOf[Array[String]].toArray
	  }
	  throw new Exception("Unhandled Collection")
	 }
    
   	def ValueToString(v: Any): String = {
		if (v.isInstanceOf[Set[_]]) {
      		return v.asInstanceOf[Set[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[List[_]]) {
      		return v.asInstanceOf[List[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[Array[_]]) {
      		return v.asInstanceOf[Array[_]].mkString(",")
	  	}
	  	v.toString
	}
    
   private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var list : List[Map[String, Any]] = null 
    var keySet: Set[Any] = Set();
	try{
	   val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
       if (mapOriginal == null)
         throw new Exception("Invalid json data")
       
       val map : scala.collection.mutable.Map[String, Any] =  scala.collection.mutable.Map[String, Any]()
       mapOriginal.foreach(kv => {map(kv._1.toLowerCase()) = kv._2 } )      
    
	  	val msgsAndCntrs : scala.collection.mutable.Map[String, Any] = null
    
	  	// Traverse through whole map and make KEYS are lowercase and populate
	  	map.foreach(kv => {
        	val key = kv._1.toLowerCase
        	val typConvidx = keys.getOrElse(key, -1)
        	if (typConvidx > 0) {
          	// Cast to correct type
          	val v1 = typs(typConvidx).Input(kv._2.toString)
	  		// println("==========v1"+v1)
          	fields.put(key, (typConvidx, v1))
        } else { // Is this key is a message or container???
          if (messagesAndContainers(key))
            msgsAndCntrs.put(key, kv._2)
          else if (collectionTypes(key)) {
            // BUGBUG:: what to dfo?
          } else
            fields.put(key, (0, ValueToString(kv._2)))
        }
      })
    
     // fields.foreach(field => println("Key : "+ field._1 + "Idx " + field._2._1 +"Value" + field._2._2 ))
   
	  } catch {
      	case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
  
   private def populateXml(xmlData:XmlData) : Unit = {
	try{
	  //XML Population is not handled at this time      
     
	}catch{
	  case e:Exception =>{
	    e.printStackTrace()
		throw e	    	
	  }
   	}
  }

    override def Serialize(dos: DataOutputStream): Unit = {
    try {
      // Base Stuff
      SerializeBaseTypes(dos)
       // Non Base Types
     SerializeNonBaseTypes(dos)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  
    private def SerializeBaseTypes(dos: DataOutputStream): Unit = {
  
    var cntCanSerialize: Int = 0
    fields.foreach(field => {
      if (field._2._1 >= 0)
        cntCanSerialize += 1
    })
    com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, cntCanSerialize)

    // Note:: fields has all base and other stuff
    fields.foreach(field => {
	  if (field._2._1 >= 0) {
      	val key = field._1.toLowerCase
      	com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, key)
      	com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, field._2._1)
      	field._2._1 match {
      		 case 0 => com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, field._2._2.asInstanceOf[String])  
	 case 1 => com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, field._2._2.asInstanceOf[Int])  

   
      		case _ => {} // could be -1
      	}
      }
    })
   }
  
    private def SerializeNonBaseTypes(dos: DataOutputStream): Unit = {
    
    }
    
    override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {
	  try {
      	if (savedDataVersion == null || savedDataVersion.trim() == "")
        	throw new Exception("Please provide Data Version")
    
      	val prevVer = savedDataVersion.replaceAll("[.]", "").toLong
      	val currentVer = Version.replaceAll("[.]", "").toLong
      	 
      	
         if(prevVer == currentVer){  
              
	  val desBaseTypes = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
	  //println("desBaseTypes "+desBaseTypes)
        for (i <- 0 until desBaseTypes) {
          val key = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis)
          val typIdx = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
          
	  	  typIdx match {
          		 case 0 => fields(key) = (typIdx, com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis));
	 case 1 => fields(key) = (typIdx, com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis));

          	case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
          }
         
       }

   
        } else throw new Exception("Current Message/Container Version "+currentVer+" should be greater than Previous Message Version " +prevVer + "." )
      
      	} catch {
      		case e: Exception => {
          		e.printStackTrace()
      		}
      	}
    } 
     
   def ConvertPrevToNewVerObj(obj : Any) : Unit = { }
    
}