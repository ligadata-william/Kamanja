package com.ligadata.olep.metadata

import scala.Enumeration
import scala.collection.mutable.{ Map, Set, TreeSet }
import scala.io.Source._
import java.util._

import scala.util.parsing.json.{ JSONObject, JSONArray }

// define some enumerations 
object ObjFormatType extends Enumeration {
  type FormatType = Value
  val fCSV, fJSON, fXML, fSERIALIZED = Value

  def asString(typ : FormatType) : String = {
     val str = typ.toString match {
	case "fCSV" =>  "CSV"
	case "fJSON" => "JSON"
	case "fXML" => "XML"
	case "fSERIALIZED" => "SERIALIZED"
	case _ => "Unknown"
      }
      str
  }

}
import ObjFormatType._

/*
object ObjContainerType extends Enumeration {
	type ContainerType = Value
	val tArray, tSet, tMap, tStruct = Value
}

object ObjScalarType extends Enumeration {
	type ScalarType = Value
	val tInt, tFloat, tDouble, tString, tBoolean = Value
}

import ObjContainerType._ 
import ObjScalarType._
*/

object ObjType extends Enumeration {
	type Type = Value
	val tNone, tAny, tInt, tLong, tFloat, tDouble, tString, tBoolean, tChar, tArray, tArrayBuf, tSet, tTreeSet, tSortedSet, tMap, tHashMap, tMsgMap, tList, tQueue, tStruct, tAttr = Value
	
	def asString(typ : Type) : String = {
		val str = typ.toString match {
			case "tNone" =>  "None"
			case "tInt" => "Int"
			case "tAny" => "Any"
			case "tLong" => "Long"
			case "tFloat" => "Float"
			case "tDouble" => "Double"
			case "tString" => "String"
			case "tBoolean" => "Boolean"
			case "tChar" => "Char"
			case "tArray" => "Array"
			case "tArrayBuf" => "ArrayBuffer"
			case "tSet" => "Set"
			case "tSortedSet" => "SortedSet"
			case "tTreeSet" => "TreeSet"
			case "tMap" => "Map"
			case "tHashMap" => "HashMap"
			case "tMsgMap" => "Map"
			case "tList" => "List"
			case "tQueue" => "Queue"
			case "tStruct" => "Struct"
			case _ => "None"
		}
		str
	}
	def fromString(typeStr : String) : Type = {
		val typ : Type = typeStr match {
			case "None" =>  tNone
			case "Any" => tAny
			case "Int" => tInt
			case "Long" => tLong
			case "Float" => tFloat
			case "Double" => tDouble
			case "String" => tString
			case "Boolean" => tBoolean
			case "Char" => tChar
			case "Array" => tArray
			case "Set" => tSet
			case "SortedSet" => tSortedSet
			case "TreeSet" => tTreeSet
			case "Map" => tMap
			case "HashMap" => tHashMap
			case "MsgMap" => tMap
			case "List" => tList
			case "Queue" => tQueue
			case "Struct" => tStruct
			case _ => tNone
		}
		typ
	}
}

import ObjType._

object ObjTypeType extends Enumeration {
  type TypeType = Value
  val tAny, tScalar, tContainer, tTupleN = Value

  def asString(typ: TypeType): String = {
    val str = typ.toString match {
      case "tAny" => "Any"
      case _ => typ.toString
    }
    str
  }
}
import ObjTypeType._

object DefaultMdElemStructVer {
  def Version = 1 // Default version is 1 
}

// case class FullName (nameSpace: String, name: String)
// case class Dates (creationTime: Date, modTime: Date)

// common fields for all metadata elements
trait BaseElem {
  def UniqID: Long
  def FullName: String // Logical Name
  def FullNameWithVer: String
  def CreationTime: Long // Time in milliseconds from 1970-01-01T00:00:00
  def ModTime: Long // Time in milliseconds from 1970-01-01T00:00:00
  def OrigDef: String
  def Description: String
  def Author: String
  def NameSpace: String
  def Name: String
  def Version: Int
  def JarName: String
  def DependencyJarNames: Array[String]
  def MdElemStructVer: Int // Metadata Element Structure version. By default whole metadata will have same number
  def PhysicalName: String // Getting Physical name for Logical name (Mapping from Logical name to Physical Name when we generate code)
  def PhysicalName(phyNm: String): Unit // Setting Physical name for Logical name (Mapping from Logical name to Physical Name when we generate code)
  def ObjectDefinition: String // Get Original XML/JSON string used during model/message compilation
  def ObjectDefinition(definition: String): Unit // Set XML/JSON Original string used during model/message compilation
  def ObjectFormat: ObjFormatType.FormatType // format type for Original string(json or xml) used during model/message compilation
  def IsActive: Boolean // Return true if the Element is active, otherwise false
  def IsDeactive: Boolean // Return true if the Element is de-active, otherwise false
  def IsDeleted: Boolean // Return true if the Element is deleted, otherwise false
  def TranId : Long // a unique number representing the transaction that modifies this object
  def Active: Unit // Make the element as Active
  def Deactive: Unit // Make the element as de-active
  def Deleted: Unit // Mark the element as deleted
}

class BaseElemDef extends BaseElem {
  override def UniqID: Long = uniqueId
  override def FullName: String = nameSpace + "." + name // Logical Name
  override def FullNameWithVer: String = nameSpace + "." + name + "." + ver
  override def CreationTime: Long = creationTime // Time in milliseconds from 1970-01-01T00:00:00
  override def ModTime: Long = modTime // Time in milliseconds from 1970-01-01T00:00:00
  override def OrigDef: String = origDef
  override def Description: String = description
  override def Author: String = author
  override def NameSpace: String = nameSpace // Part of Logical Name
  override def Name: String = name // Part of Logical Name
  override def Version: Int = ver
  override def JarName: String = jarName
  override def DependencyJarNames: Array[String] = dependencyJarNames
  override def MdElemStructVer: Int = mdElemStructVer // Metadata Element version. By default whole metadata will have same number
  override def PhysicalName: String = physicalName // Getting Physical name for Logical name (Mapping from Logical name to Physical Name when we generate code)
  override def PhysicalName(phyNm: String): Unit = physicalName = phyNm // Setting Physical name for Logical name (Mapping from Logical name to Physical Name when we generate code). Most of the elements will have Phsical name corresponds to Logical name like Types like System.Int maps to scala.Int as physical name.
  override def ObjectDefinition: String = objectDefinition // Original XML/JSON string used during model/message compilation
  override def ObjectDefinition(definition: String): Unit = objectDefinition = definition // Set XML/JSON Original string used during model/message compilation
  override def ObjectFormat: ObjFormatType.FormatType = objectFormat // format type for Original string(json or xml) used during model/message compilation
  override def IsActive: Boolean = active // Return true if the Element is active, otherwise false
  override def IsDeactive: Boolean = (active == false) // Return true if the Element is de-active, otherwise false
  override def IsDeleted: Boolean = (deleted == true) // Return true if the Element is deleted, otherwise false
  override def TranId : Long = tranId// a unique number representing the transaction that modifies this object
  override def Active: Unit = active = true // Make the element as Active
  override def Deactive: Unit = active = false // Make the element as de-active
  override def Deleted: Unit = deleted = true // Mark the element as deleted

  // Override in other places if required
  override def equals(that: Any) = {
    that match {
      case f: BaseElemDef => f.FullNameWithVer == FullNameWithVer
      case _ => false
    }
  }

  var uniqueId: Long = 0
  var creationTime: Long = _ // Time in milliseconds from 1970-01-01T00:00:00 (Mostly it is Local time. May be we need to get GMT) 
  var modTime: Long = _ // Time in milliseconds from 1970-01-01T00:00:00 (Mostly it is Local time. May be we need to get GMT)

  var origDef: String = _ // string associated with this definition 
  var description: String = _
  var author: String = _
  var nameSpace: String = _ //
  var name: String = _ // simple name - may not be unique across all name spaces (coupled with mNameSpace, it will be unique)
  var ver: Int = _ // version number - nn.nn.nn form (without decimal)
  var jarName: String = _ // JAR file name in which the generated metadata info is placed (classes, functions, etc.,)
  var dependencyJarNames: Array[String] = _ // These are the dependency jars for this
  var mdElemStructVer: Int = DefaultMdElemStructVer.Version // Metadata Element Structure version. By default whole metadata will have same number
  var physicalName: String = _ // Mapping from Logical name to Physical Name when we generate code. This is Case sensitive.
  var active: Boolean = true // Represent whether element is active or deactive. By default it is active.
  var deleted: Boolean = false // Represent whether element is deleted. By default it is false.
  var tranId: Long = 0	
  var objectDefinition:String = _
  var objectFormat: ObjFormatType.FormatType = fJSON
}

// All these metadata elements should have specialized serialization and deserialization 
// functions when storing in key/value store as many member objects should be stored as reference rather than entire object

trait TypeDefInfo {
  def tTypeType: TypeType // type of type
  def tType: Type
}

trait TypeImplementation[T] {
  def Input(value: String): T // Converts String to Type T
  def Serialize(value: T): Array[Byte] // Convert Type T to Array[Byte]
  def Deserialize(value: Array[Byte]): T // Convert Array[Byte] to Type T
  def toString(value: T): String // Convert Type T to String
  def toJsonString(value: T): String // Convert Type T to Json String
}

abstract class BaseTypeDef extends BaseElemDef with TypeDefInfo {
  def typeString: String = PhysicalName // default PhysicalName

  def implementationName: String = implementationNm // Singleton object name/Static Class name of TypeImplementation
  def implementationName(implNm: String): Unit = implementationNm = implNm // Setting Implementation Name

  var implementationNm: String = _ // Singleton object name/Static Class name of TypeImplementation
}

// basic type definition in the system
class ScalarTypeDef extends BaseTypeDef {
  def tTypeType = tScalar
  def tType = typeArg;

  var typeArg: Type = _
}

class AnyTypeDef extends BaseTypeDef {
  def tTypeType: ObjTypeType.TypeType = ObjTypeType.tAny
  def tType = tNone

  override def typeString: String = {
    "Any" 
  }
}

abstract class ContainerTypeDef extends BaseTypeDef {
  def tTypeType = tContainer

  def IsFixed: Boolean
  /** 
   *  Answer the element type or types that are held in the ContainerTypeDef subclass.
   *  This is primarily used to understand the element types of collections.  An array
   *  is returned since tuples and maps to name two have more than one element 
   *  
   *  While all ContainerTypeDef subclasses implement this, this is really used 
   *  to describe only those container based upon one of the collection classes that
   *  have element types as part of their type specification.  Specifically
   *  the MappedMsgTypeDef and StructTypeDef both use this default behavior.  See
   *  the respective classes for how to gain access to the field domain and fields
   *  they possess.
   *  
   *  @return As a default, give a null list.  
   */
  def ElementTypes : Array[BaseTypeDef] = Array[BaseTypeDef]()
}

class SetTypeDef extends ContainerTypeDef {
  def tType = tSet
  var keyDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.Set[" + keyDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef)
  }
}

class ImmutableSetTypeDef extends ContainerTypeDef {
  def tType = tSet
  var keyDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.immutable.Set[" + keyDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef)
  }
}

class TreeSetTypeDef extends ContainerTypeDef {
  def tType = tTreeSet
  var keyDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.TreeSet[" + keyDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef)
  }
}

class SortedSetTypeDef extends ContainerTypeDef {
  def tType = tSortedSet
  var keyDef : BaseTypeDef = _
   
  override def IsFixed : Boolean = false
  override def typeString : String = {
	 "scala.collection.mutable.SortedSet[" + keyDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef)
  }
}

class MapTypeDef extends ContainerTypeDef {
  def tType = tMap

  var keyDef: BaseTypeDef = _
  var valDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.Map[" + keyDef.typeString + "," + valDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef,valDef)
  }
}

class ImmutableMapTypeDef extends ContainerTypeDef {
  def tType = tMap

  var keyDef: BaseTypeDef = _
  var valDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.immutable.Map[" + keyDef.typeString + "," + valDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef,valDef)
  }
}

class HashMapTypeDef extends ContainerTypeDef {
  def tType = tHashMap

  var keyDef: BaseTypeDef = _
  var valDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.HashMap[" + keyDef.typeString + "," + valDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(keyDef,valDef)
  }
}

class ListTypeDef extends ContainerTypeDef {
  def tType = tList
  var valDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.List[" + valDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(valDef)
  }
}

class QueueTypeDef extends ContainerTypeDef {
  def tType = tQueue
  var valDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.Queue[" + valDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(valDef)
  }
}

class ArrayTypeDef extends ContainerTypeDef {
  def tType = tArray

  var arrayDims: Int = 0 // 0 is invalid; 1..N - dimensions - indicate array of that many dimensions
  var elemDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.Array[" + elemDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(elemDef)
  }
}

class ArrayBufTypeDef extends ContainerTypeDef {
  def tType = tArrayBuf

  var arrayDims: Int = 0 // 0 is invalid; 1..N - dimensions - indicate array of that many dimensions
  var elemDef: BaseTypeDef = _

  override def IsFixed: Boolean = false
  override def typeString: String = {
    "scala.collection.mutable.ArrayBuffer[" + elemDef.typeString + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	Array(elemDef)
  }
}

class TupleTypeDef extends ContainerTypeDef {
  override def tTypeType = tTupleN
  def tType: ObjType.Type = ObjType.tAny

  var tupleDefs: Array[BaseTypeDef] = Array[BaseTypeDef]()

  override def IsFixed: Boolean = false
  override def typeString: String = {
    val sz : Int = tupleDefs.size
    s"scala.Tuple$sz[" + tupleDefs.map(tup => tup.typeString).mkString(",") + "]"
  }
  override def ElementTypes : Array[BaseTypeDef] = {
	tupleDefs
  }
}

object RelationKeyType extends Enumeration {
  type RelationKeyType = Value
  val tPrimary, tForeign = Value
  def asString(typ: RelationKeyType): String = {
    typ.toString
  }
}
import RelationKeyType._

abstract class RelationKeyBase {
  var constraintName:String = _ // If we have given any name for this constraint
  var key:Array[String] = _ // Local Primary Key / Foreign Key Field Names
  def KeyType: RelationKeyType // Relation type could be Primary / Foreign at this moment
}

class PrimaryKey extends RelationKeyBase {
  def KeyType: RelationKeyType = tPrimary
}

class ForeignKey extends RelationKeyBase {
  def KeyType: RelationKeyType = tForeign
  var forignContainerName:String = _// Container or Message Name
  var forignKey:Array[String] = _ // Names in Foreign Container (which are primary keys there). Expecting same number of names in key & forignKey
}

trait EntityType {
  var keys: Array[RelationKeyBase] = _ // Keys (primary & foreign keys) for this container. For now we are consider them for MAP based and STRUCT based containers.
  var partitionKey: Array[String] = _ // Partition Key (attribute names)
  def NumMems
  def Keys = keys
  def PartitionKey = partitionKey
}

class MappedMsgTypeDef extends ContainerTypeDef with EntityType {
	def tType = tMsgMap

	var attrMap : Map[String, BaseAttributeDef] = Map[String, BaseAttributeDef]()

  	override def NumMems = attrMap.size
  	override def IsFixed: Boolean = false
  	def attributeFor(name : String) : BaseAttributeDef = { 
	  	val key = name.toLowerCase()
	  	val hasName : Boolean = attrMap.contains(key)
		val baseAttrDef : BaseAttributeDef = if (hasName) {
			attrMap.apply(key) 
		}  else {
			null
		}
	  	baseAttrDef
  	}
}

class StructTypeDef extends ContainerTypeDef with EntityType {
  def tType = tStruct

  var memberDefs: Array[BaseAttributeDef] = _

  override def NumMems = memberDefs.size
  override def IsFixed: Boolean = true
  def attributeFor(name : String) : BaseAttributeDef = { 
	  val key = name.toLowerCase()
      val optMbr : Option[BaseAttributeDef] = memberDefs.find( m => m.name == key)
      val mbr : BaseAttributeDef = optMbr match {
		case Some(optMbr) => optMbr
		case _ => null
     }
     mbr
  }
}

// attribute/concept definition
abstract class BaseAttributeDef extends BaseElemDef {
  def parent: BaseAttributeDef
  def typeDef: BaseTypeDef //BaseElemDef

  def typeString: String
}

class AttributeDef extends BaseAttributeDef {
  def tType = tAttr
  def tTypeType = tContainer
  def parent = inherited
  override def typeDef : BaseTypeDef = aType

  var aType: BaseTypeDef = _
  var inherited: AttributeDef = _ // attributes could be inherited from others - in that case aType would be same as parent one
  var collectionType = tNone // Fill if there is collection type for this attribute

  override def typeString: String = {
    val baseTypStr = if (parent != null) parent.typeString else aType.typeString
    if (collectionType == tNone) {
      baseTypStr
    } else {
      if (collectionType == tArray) {
        "Array[" + baseTypStr + "]"
      } else if (collectionType == tArrayBuf) {
        "scala.collection.mutable.ArrayBuffer[" + baseTypStr + "]"
      } else {
        throw new Throwable(s"Not yet handled collection Type $collectionType")
      }
    }
  }
}

// attribute/concept definition
class DerivedAttributeDef extends AttributeDef {
  def func = funcDef
  def baseAttribs = baseAttribDefs

  var funcDef: FunctionDef = _
  var baseAttribDefs: Array[AttributeDef] = _ // list of attributes on which this attribute is derived from (arguments to function)
}

class ContainerDef extends BaseElemDef {
  def cType = containerType

  var containerType: EntityType = _ // container structure type -

  def typeString: String = PhysicalName
}

class MessageDef extends ContainerDef {
}

class ArgDef {
  def Type = aType
  var name: String = _
  var aType: BaseTypeDef = _ // simple scalar types, array of scalar types, map/set

  def DependencyJarNames: Array[String] = {
    if (aType.JarName == null && aType.DependencyJarNames == null) {
      null
    } else {
      val depJarSet = scala.collection.mutable.Set[String]()
      if (aType.JarName != null) depJarSet += aType.JarName
      if (aType.DependencyJarNames != null) depJarSet ++= aType.DependencyJarNames
      if (depJarSet.size > 0) depJarSet.toArray else null
    }
  }
  def typeString: String = aType.typeString
}

class FunctionDef extends BaseElemDef {
  var retType: BaseTypeDef = _ // return type of this function - could be simple scalar or array or complex type such as map or set
  var args: Array[ArgDef] = _ // list of arguments definitions
  var className: String = _ // class name that has this function?
  var features: Set[FcnMacroAttr.Feature] = Set[FcnMacroAttr.Feature]()

  // Override in other places if required
  override def equals(that: Any) = {
    that match {
      case f: FunctionDef => (f.FullNameWithVer == FullNameWithVer && f.args.size == args.size &&
        (f.args.map(arg => arg.Type.FullName).mkString(",") == args.map(arg => arg.Type.FullName).mkString(",")))
      case _ => false
    }
  }
  def typeString : String = (FullName + "(" + args.map(arg => arg.Type.typeString).mkString(",") + ")").toLowerCase
  //def tStr: String = (FullName + "(" + args.map(arg => arg.Type.FullName).mkString(",") + ")").toLowerCase

  def returnTypeString: String = if (retType != null) retType.typeString else "Unit"
  //def AnotherImplementationForReturnTypeString: String = if (retType != null) retType.tStr else "Unit"
    
  def isIterableFcn : Boolean = { features.contains(FcnMacroAttr.ITERABLE) }
}

/** 
 *  The FcnMacroAttr.Feature is used to describe the sort of macro is being defined.  Briefly,
 *  
 *    ITERABLE - when included in a MacroDef instance's features set, the first argument of the macro
 *    	is a Scala Iterable and the code that will be generated looks like arg1.filter( itm => arg2(arg3,arg4,...,argN)
 *      or other iterable function (e.g., map, foldLeft, zip, etc).  
 *    CLASSUPDATE - when designated in the features set, it indicates the macro will update its first argument as a side effect 
 *    	and return whether the update happened as a Boolean.  This is needed so that the variable updates can be folded into 
 *      the flow of a pmml predicate interpretation.  Variable updates are currently done inside a class as a variable arg
 *      to the constructor.  These classes are added to the current derived field class before the enclosing '}' for the
 *      class representing the derived field.  A global optimization of the derived field's function would be needed to 
 *      do a better job by reorganizing the code and possibly breaking the top level derived function into multiple 
 *      parts. 
 */
object FcnMacroAttr extends Enumeration {
	type Feature = Value
	val ITERABLE, CLASSUPDATE = Value
}

class MacroDef extends FunctionDef {
  /** 
   *  This is the template text with subsitution variables embedded.  These
   *  are demarcated with "%" ... e.g., %variable%.  Variable symbol names can have most characters
   *  in them including .+_, ... no support for escaped % at this point.  
   *  
   *  Note: There are two templates.  One is for the containers with fixed fields.  The other is for
   *  the so-called mapped containers that use a dictionary to represent sparse fields.  Obviously
   *  some macros don't have containers as one of their elements.  In this case, the same template 
   *  populates both members of the tuple.  See MakeMacro in mdmgr.scala for details.
   */
  var macroTemplate: (String,String) = ("","")
}

class ModelDef extends BaseElemDef {
  var modelType: String = _ // type of models (RuleSet,..)
  var inputVars: Array[BaseAttributeDef] = _
  var outputVars: Array[BaseAttributeDef] = _

  def typeString: String = PhysicalName
}


class JarDef extends BaseElemDef{
  def typeString: String = PhysicalName
}


class NodeInfo {
  /**
   * This object captures the information related to a node within a cluster
   */
  var nodeId: String = _
  var nodePort: Int = _
  var nodeIpAddr: String = _
  var jarPaths: Array[String] = new Array[String](0)
  var clusterId: String = _
  var power:Int = _
  var roles:Int = _
  var description: String = _

  def NodeId: String = nodeId
  def NodePort: Int = nodePort
  def NodeIpAddr: String = nodeIpAddr
  def JarPaths: Array[String] = jarPaths
  def Key: String = nodeId
  def ClusterId: String = clusterId
  def Power: Int = power
  def Roles: Int = roles
  def Description: String = description
}

class ClusterInfo {
  /**
   * This object captures the information related to a cluster
   */
  var clusterId: String = _
  var description: String = _
  var privileges: String = _

  def ClusterId: String = clusterId
  def Description: String = description
  def Privileges: String = privileges
}

class ClusterCfgInfo {
  /**
   * This object captures the information related to a clusterConfiguration
   */
  var clusterId: String = _
  var cfgName: String = _
  var cfgValue : String = _
  var modifiedTime: Date = _
  var createdTime: Date = _

  def ClusterId: String = clusterId
  def CfgName: String = cfgName
  def CfgValue: String = cfgValue
  def ModifiedTime: Date = modifiedTime
  def CreatedTime: Date = createdTime
}


class AdapterInfo {
  /**
   * This object captures the information related to a adapters used by Engine
   */
  var name: String = _
  var typeString: String = _
  var dataFormat: String = _
  var className: String = _
  var jarName: String = _
  var dependencyJars: Array[String] = new Array[String](0)
  var fileName: String = _
  var adapterSpecificCfg: String = _

  def Name: String = name
  def TypeString: String = typeString
  def DataFormat: String = dataFormat
  def ClassName: String = className
  def JarName: String = jarName
  def DependencyJars: Array[String] = dependencyJars
  def FileName: String = fileName
  def AdapterSpecificCfg: String = adapterSpecificCfg
}
