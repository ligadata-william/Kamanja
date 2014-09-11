package com.ligadata.MetadataAPI

object SampleData {

  val sampleMessageDef1 = """
  {
	"Message": { 
	  "Name":"BankPocMsg",
	  "Version":"1",
	  "Description":"BankPocMsg",
	  "IsFixed":"true",
	  "IsKv":"false",
	  "Fields" : [
				{
					"Name":"ENT_DTE",
					"Type":"Int"
				},
				{
					"Name":"ENT_ACC_NUM",
					"Type":"Long"
				},
				{
					"Name":"RUN_LDG_XAU",
					"Type":"Float"
				},
				{
					"Name":"ODR_LMT",
					"Type":"Double"
				},
				{
					"Name":"ENT_AMT",
					"Type":"Double"
				},
				{
					"Name":"ANT_LMT",
					"Type":"Double"
				},
				{
					"Name":"BNT_LMT",
					"Type":"Double"
				}
			]
      	}
      }
 """

  val sampleMessageDef2 = """{
   	"Message": { 
   		"NameSpace":"Ligadata",
   		"Name":"OutpatientClaimMsg",
   		"Version":"00.01.00",
   		"Description":"Outpatient Claim",
   		"Fixed":"true",
   		"TransformData" : {
   			"Input" : [
   				"Desynpuf_Id", 
   				"Name", 
   				"Clm_Id", 
   				"Filed1", 
   				"Clm_From_Dt", 
   				"Filed2", 
   				"Clm_Thru_Dt", 
   				"Segment", 
   				"Prvdr_Num"
   			],
   			"Output" : [
   				"Desynpuf_Id",
   				"Clm_Id",
   				"Clm_From_Dt",
   				"Clm_Thru_Dt",
   				"Prvdr_Num",
   				"Segment"
   			],
   			"Keys" : [
   				"Desynpuf_Id"
   			]
   		},
  		"Elements" : [
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Desynpuf_Id",
                "Type": "System.String"
   		     }
   		   },
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Clm_Id",
                "Type": "System.Long"
   		     }
   		   },
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Clm_From_Dt",
                "Type": "System.Int"
   		     }
   		   },
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Clm_Thru_Dt",
                "Type": "System.Int"
   		     }
   		   },
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Prvdr_Num",
                "Type": "System.String"
   		     }
   		   },
   		   {
   		   "Field" : {
   		      "NameSpace":"Ligadata",
                "Name": "Segment",
                "Type": "System.Int"
   		     }
   		   },
   		   {
   		   "Container" : "Ligadata.Provider"
   		   }
   		]
   	}
   }"""

    val sampleFunctionsText = 
      """{ "Functions":
                         [
   				{
   				"Name":"abs",
   				"Version":"1",
   				"ReturnType":"Int",
   				"Arguments": 
				[
				{
   				       "Name": "val",
   				       "Type": "Int"
				}
   				],
   				"Implementation": {
				"Name":"abs_int_long_1.jar",
				"Type":"jar",
   				   "Depends": [
                                    {
                                     "Name":"dep1.jar",
                                     "Type":"jar"
                                    },
   				    {
                                      "Name":"dep2.jar",
                                      "Type":"jar"
                                    }
   				    ]
   				  }
   				},
   				{
   				"Name":"abs",
   				"Version":"1",
   				"ReturnType":"Long",
   				"Arguments": [
				  {
   				       "Name": "val",
   				       "Type": "Long"
				  }
   				],
   				"Implementation": {
				"Name":"abs_int_long_1.jar",
				"Type":"jar",
   				"Depends": [
   				 {
                                  "Name":"dep1.jar",
                                  "Type":"jar"
                                 },
   				 {
                                  "Name":"dep2.jar",
                                  "Type":"jar"
                                 }
				]
   				}
				}
			]
	      }"""


  var sampleScalaStr = """
package com.ligadata.olep.metadata
import scala.Enumeration
import scala.collection.mutable.{Map}
import scala.io.Source._
import java.util._

import scala.util.parsing.json.{JSONObject, JSONArray}

// define some enumerations 
object ObjFormatType extends Enumeration {
	type FormatType = Value
	val fCSV, fJSON, fSERIALIZED = Value
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
	val tNone, tInt, tLong, tFloat, tDouble, tString, tBoolean, tArray, tSet, tMap, tMsgMap, tList, tStruct = Value
	
	def asString(typ : Type) : String = {
		val str = typ.toString match {
			case "tNone" =>  "None"
			case "tInt" => "Int"
			case "tLong" => "Long"
			case "tFloat" => "Float"
			case "tDouble" => "Double"
			case "tString" => "String"
			case "tBoolean" => "Boolean"
			case "tArray" => "Array"
			case "tSet" => "Set"
			case "tMap" => "Map"
			case "tMsgMap" => "Map"
			case "tList" => "List"
			case "tStruct" => "Struct"
			case _ => "None"
		  
		}
		str
	}
}

import ObjType._

object ObjTypeType extends Enumeration {
  type TypeType = Value
  val tAny, tScalar, tContainer, tTupleN = Value
}
import ObjTypeType._

case class FullName (nameSpace: String, name: String)
case class Dates (creationTime: Date, modTime: Date)

// common fields for all metadata elements
trait BaseElem {
  def UniqID: Long
  def FullName: String
  def CreationTime: Date
  def ModTime: Date
  def OrigDef: String
  def Description: String
  def Author: String
  def NameSpace: String
  def Name: String
  def Version: Int
  def JarName: String
}

class BaseElemDef extends BaseElem {
  def UniqID : Long = uniqueId
  def FullName: String = nameSpace + ":" + name + ":" + ver
  def CreationTime: Date = creationTime
  def ModTime: Date = modTime
  def OrigDef: String = origDef
  def Description: String = description
  def Author: String = author
  def NameSpace: String = nameSpace
  def Name: String = name
  def Version: Int = ver
  def JarName: String = jarName

  var uniqueId : Long = 0
  var creationTime: Date = _     // date and time
  var modTime: Date = _
  
  var origDef: String = _       // string associated with this definition 
  var description: String = _     
  var author: String = _
  var nameSpace: String	= _	//
  var name : String = _			// simple name - may not be unique across all name spaces (coupled with mNameSpace, it will be unique)
  var ver : Int = _				// version number - nn.nn.nn form (without decimal)
  var jarName: String = _	    // JAR file name in which the generated metadata info is placed (classes, functions, etc.,)
} 

// All these metadata elements should have specialized serialization and deserialization 
// functions when storing in key/value store as many member objects should be stored as reference rather than entire object

trait TypeDefInfo { 
  def tTypeType : TypeType      // type of type
  def tType :  Type
}

abstract class BaseTypeDef extends BaseElemDef with TypeDefInfo {
  
  def makeKey : String
}

/*
// basic type definition in the system
class TypeDef extends BaseElemDef {
  var typeType : TypeType       // type of type
  var arrayDims: Int            // 0 indicates non array; 1..N - dimensions - indicate array of that many dimensions
  // set of arguments needed based on  type
  //   tScalar = typeArgs[0] indicates scalar type (Int, Float, Boolean, etc.,)
  //   tAny    = none
  //   tMap    = typeArgs[0] is key type and typeArgs[1] is val type
  //   tSet    = typeArgs[0] is the key type
  //   tStruct = typeArgs[0]..typeArgs[N] are AttributeDef keys
  var typeArgs : Array[String]	  
}
*/

// trait ScalarTypeInfo 	 { def getType : ScalarType    }
// trait ContainerTypeInfo  { def getType : ContainerType }

// basic type definition in the system
class ScalarTypeDef extends BaseTypeDef { 
  def tTypeType = tScalar
  def tType = typeArg;
  
  var typeArg : Type = _

  def makeKey : String = {
	 ObjType.asString(typeArg)
  }
}

abstract class ContainerTypeDef extends BaseTypeDef {
  def tTypeType = tContainer

  def IsFixed : Boolean
  def makeKey : String = {
	 name
  }
}

class AnyTypeDef extends BaseTypeDef {
  def tTypeType = tAny;
  def tType = tNone

  def makeKey : String = {
	 "Any"
  }
}

class SetTypeDef extends ContainerTypeDef {
  def tType = tSet
  var keyDef : BaseTypeDef = _
   
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "List[" + keyDef.toString + "]"
  }
}

class MapTypeDef extends ContainerTypeDef {
  def tType = tMap
  
  var keyDef : BaseTypeDef = _
  var valDef : BaseTypeDef = _
  
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "Map[" + keyDef.toString + "," + valDef.toString + "]"
  }
}

class MappedMsgTypeDef extends ContainerTypeDef {
  def tType = tMsgMap
  
  var keyDef : BaseTypeDef = _
  var valDef : BaseTypeDef = _
  
  def NumMems = attrMap.size
  var attrMap: Map[String, AttributeDef] = Map[String, AttributeDef]()
  
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "Map[" + keyDef.toString + "," + valDef.toString + "]"
  }
}

class ListTypeDef extends ContainerTypeDef {
  def tType = tList
  var valDef : BaseTypeDef = _
  
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "List[" + valDef.toString + "]"
  }
}

class StructTypeDef extends ContainerTypeDef {
  def tType = tStruct
  
  def NumMems = memberDefs.size
  var memberDefs: Array[AttributeDef] = _

  override def IsFixed : Boolean = true
  override def makeKey : String = {
	  name
  }

}

class ArrayTypeDef extends ContainerTypeDef {
  def tType = tArray
  
  var arrayDims: Int = 0           // 0 is invalid; 1..N - dimensions - indicate array of that many dimensions
  var elemDef : BaseTypeDef = _
  
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "Array[" + elemDef.toString + "]"
  }
}

class ArrayBufTypeDef extends ContainerTypeDef {
  def tType = tArray
  
  var arrayDims: Int = 0           // 0 is invalid; 1..N - dimensions - indicate array of that many dimensions
  var elemDef : BaseTypeDef = _
  
  override def IsFixed : Boolean = false
  override def makeKey : String = {
	 "ArrayBuffer[" + elemDef.toString + "]"
  }
}

/*
// does it make sense that function 
class FuncTypeDef extends BaseTypeDef {
}
*/

// attribute/concept definition
abstract class BaseAttributeDef extends BaseElemDef with TypeDefInfo {
  def parent : BaseAttributeDef
  def typeDef : BaseTypeDef
  
  def makeKey : String
}

class AttributeDef extends BaseAttributeDef with TypeDefInfo {
  def tType = aType.tType
  def tTypeType = aType.tTypeType
  def parent = inherited
  def typeDef = aType
  
  var aType: BaseTypeDef = _
  var inherited: AttributeDef = _	// attributes could be inherited from others - in that case aType would be same as parent one
  
  override def makeKey : String = {
	  aType.makeKey
  }
}

// attribute/concept definition
class DerivedAttributeDef extends AttributeDef {
  def func = funcDef
  def baseAttribs = baseAttribDefs
  
  var funcDef: FunctionDef = _
  var baseAttribDefs: Array[AttributeDef] = _		// list of attributes on which this attribute is derived from (arguments to function)
}

class ContainerDef extends BaseElemDef {
  def cType = containerType
  def classNm = className
  
  var containerType : ContainerTypeDef = _		// container structure type -  
  var className  : String	= _	// generated class name representing this container
  
  def makeKey : String = {
	  className
  }
}

class MessageDef extends ContainerDef {
  var mType : String = _	// type of message specified in the definition - full qualified type specifier (e.g., edifecs.inpatient)
}

class ArgDef {
  def Type = aType
  var name: String = _
  var aType: BaseTypeDef = _     // simple scalar types, array of scalar types, map/set
  
  def makeKey : String = {
	  aType.makeKey
  }
}
  
class FunctionDef extends BaseElemDef {
  override def FullName : String = {
    super.FullName + ":" + args.foldLeft("")((fullNm, elem) => fullNm + ":" + elem.Type.FullName)
  }
  var retType: BaseTypeDef = _    // return type of this function - could be simple scalar or array or complex type such as map or set
  var args: Array[ArgDef] = _     // list of arguments definitions
  var className: String   = _     // class name that has this function?
  var isIterable : Boolean = false
  
  def makeKey : String = {
    val sb : StringBuilder = new StringBuilder()
    sb.append(s"$nameSpace.$name(")
    val stringArgs : Array[String] = args.map( arg => arg.toString )
    stringArgs.addString(sb, ",")
    sb.append(")")
    sb.toString
  }
}

class ModelDef extends BaseElemDef {
  var className: String   = _     		// class name representing this model
  var modelType: String   = _			// type of models (RuleSet,..)
  var inputVars: Array[AttributeDef] = _
  var outputVars: Array[AttributeDef] = _
}
"""

  val sampleIntTypeStr = """
    {
  	"Type": { 
  		"Name":"Int",
  		"Version":"1",
  		"Description":"Integer Type",
                "Default":"",
  		"InputFunction":"InputFunctions.OptInt",
  		"OutputFunction":"OutputFunctions.OptInt",
  		"SerializeFunction":"SerializeFunctions.OptInt",
  		"DeserializeFunction":"DeserializeFunctions.OptInt",
  		"JarName":"IntType_1.jar"
  	}
    }
  """


  val sampleTypesStr = """
    { "Types" : [
  	 { 
                "NameSpace":"System",
  		"Name":"Int",
  		"Version":"1",
  		"Description":"Integer Type",
                "Default":"0",
  		"Implementation":"IntImplementation",
  		"ImplementationName":"IntType_1.jar",
  		"ImplementationType":"jar"

  	},
  	 { 
                "NameSpace":"System",
  		"Name":"Long",
  		"Version":"1",
  		"Description":"Long Type",
                "Default":"0",
  		"Implementation":"LongImplementation",
  		"ImplementationName":"LongType_1.jar",
  		"ImplementationType":"jar"

  	},
  	 { 
                "NameSpace":"System",
  		"Name":"Float",
  		"Version":"1",
  		"Description":"Float Type",
                "Default":"0.0",
  		"Implementation":"FloatImplementation",
  		"ImplementationName":"FloatType_1.jar",
  		"ImplementationType":"jar"

  	}
    ]
  }
  """

  val sampleConceptsStr = """
  {
   	"Concepts" :  [
			{
		        "NameSpace":"Ligadata",
				"Name":"ProviderId",
				"Type":"System.String"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"ProviderName",
				"Type":"System.String"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Desynpuf_Id",
				"Type":"System.String"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Clm_Id",
				"Type":"System.Long"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Clm_From_Dt",
				"Type":"System.Int"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Clm_Thru_Dt",
				"Type":"System.Int"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Prvdr_Num",
				"Type":"System.String"
			},
			{
		        "NameSpace":"Ligadata",
				"Name":"Segment",
				"Type":"System.Int"
			}
	]
  }"""

  val sampleUpdatedIntTypeStr = """
    {
  	"Type": { 
  		"Name":"Int",
  		"Version":"1",
  		"Description":"Integer Type",
                "Default":"",
  		"InputFunction":"InputFunctions.OptInt",
  		"OutputFunction":"OutputFunctions.OptInt",
  		"SerializeFunction":"SerializeFunctions.OptInt",
  		"DeserializeFunction":"DeserializeFunctions.OptInt",
  		"JarName":"IntType_2.jar"
  	}
    }
  """

  val sampleModelInXml = """
  <model>
    <modelName>DebitLimitAlert</modelName>
    <modelVersion>1</modelVersion>
    <modelType>RuleSet</modelType>
    <jarName>DebitLimitAlert.jar</jarName>
  </model>
  """
}

