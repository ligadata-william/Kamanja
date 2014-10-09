package com.ligadata.Serialize

import com.ligadata.olep.metadata.ObjType._
import com.ligadata.olep.metadata._
import com.ligadata.olep.metadata.MdMgr._

import org.apache.log4j._

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

case class JsonException(message: String) extends Exception(message)

case class TypeDef(MetadataType:String, NameSpace: String, Name: String, TypeTypeName:String, TypeNameSpace: String, TypeName:String, PhysicalName:String, var Version: String, JarName: String, DependencyJars: List[String], Implementation: String, Fixed: Option[Boolean], NumberOfDimensions :Option[Int], KeyTypeNameSpace: Option[String], KeyTypeName: Option[String], ValueTypeNameSpace: Option[String], ValueTypeName: Option[String], TupleDefinitions: Option[List[TypeDef]] )
case class TypeDefList(Types: List[TypeDef])

case class Argument(ArgName: String, ArgTypeNameSpace:String, ArgTypeName: String)
case class Function(NameSpace:String, Name:String, PhysicalName: String, ReturnTypeNameSpace: String, ReturnTypeName: String, Arguments: List[Argument], Version:String, JarName: String, DependantJars: List[String])
case class FunctionList(Functions: List[Function])

//case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String,Description: String, Author: String, ActiveDate: String)
case class Concept(NameSpace: String,Name: String, TypeNameSpace: String, TypeName: String,Version: String)
case class ConceptList(Concepts: List[Concept])

case class Attr(NameSpace: String,Name: String, Version: Int,CollectionType: Option[String],Type: TypeDef)
case class DerivedConcept(FunctionDefinition: Function, Attributes: List[Attr])

case class MessageStruct(NameSpace: String,Name: String, FullName: String, Version: Int, JarName: String, PhysicalName: String, DependencyJars: List[String], Attributes: List[Attr])
case class MessageDefinition(Message: MessageStruct)
case class ContainerDefinition(Container: MessageStruct)

case class ModelInfo(NameSpace: String,Name: String,Version: String,ModelType: String, JarName: String,PhysicalName: String, DependencyJars: List[String], InputAttributes: List[Attr], OutputAttributes: List[Attr])
case class ModelDefinition(Model: ModelInfo)

case class UnsupportedObjectException(e: String) extends Throwable(e)
case class Json4sParsingException(e: String) extends Throwable(e)
case class FunctionListParsingException(e: String) extends Throwable(e)
case class FunctionParsingException(e: String) extends Throwable(e)
case class TypeDefListParsingException(e: String) extends Throwable(e)
case class TypeParsingException(e: String) extends Throwable(e)
case class TypeDefProcessingException(e: String) extends Throwable(e)
case class ConceptListParsingException(e: String) extends Throwable(e)
case class ConceptParsingException(e: String) extends Throwable(e)
case class MessageDefParsingException(e: String) extends Throwable(e)
case class ContainerDefParsingException(e: String) extends Throwable(e)
case class ModelDefParsingException(e: String) extends Throwable(e)
case class ApiResultParsingException(e: String) extends Throwable(e)
case class UnexpectedMetadataAPIException(e: String) extends Throwable(e)
case class ObjectNotFoundException(e: String) extends Throwable(e)
case class CreateStoreFailedException(e: String) extends Throwable(e)

// The implementation class
object JsonSerializer {
    
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[FunctionListParsingException])
  def parseFunctionList(funcListJson:String,formatType:String) : Array[FunctionDef] = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(funcListJson)

      logger.trace("Parsed the json : " + funcListJson)
      val funcList = json.extract[FunctionList]
      val funcDefList = new Array[FunctionDef](funcList.Functions.length)

      funcList.Functions.map( fn => {
	try{
	  val argList = fn.Arguments.map(arg => (arg.ArgName,arg.ArgTypeNameSpace,arg.ArgTypeName))
	  val func = MdMgr.GetMdMgr.MakeFunc(fn.NameSpace,fn.Name,fn.PhysicalName,
					   (fn.ReturnTypeNameSpace,fn.ReturnTypeName),
					   argList,null,
					   fn.Version.toInt,
					   fn.JarName,
					   fn.DependantJars.toArray)
	  funcDefList :+ func
	} catch {
	  case e:AlreadyExistsException => {
	    val funcDef = List(fn.NameSpace,fn.Name,fn.Version)
	    val funcName = funcDef.mkString(",")
	    logger.trace("Failed to add the func: " + funcName  + ": " + e.getMessage())
	  }
	}
      })
      funcDefList
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new FunctionListParsingException(e.getMessage())
      }
    }
  }


  def processTypeDef(typ: TypeDef) : BaseTypeDef = {
    var typeDef:BaseTypeDef = null
    try{
      typ.MetadataType match {
	case "ScalarTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeScalar(typ.NameSpace,typ.Name,ObjType.fromString(typ.TypeName),
				    typ.PhysicalName,typ.Version.toInt,typ.JarName,
				    typ.DependencyJars.toArray,typ.Implementation)
	}
	case "ArrayTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeArray(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				   typ.TypeName,typ.NumberOfDimensions.get,
				   typ.Version.toInt)
	}
	case "ArrayBufTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeArrayBuffer(typ.NameSpace,typ.Name,typ.TypeNameSpace,
					 typ.TypeName,typ.NumberOfDimensions.get,
					 typ.Version.toInt)
	}
	case "ListTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeList(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				  typ.TypeName,typ.Version.toInt)
	}
	case "QueueTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeQueue(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				   typ.TypeName,typ.Version.toInt)
	}
	case "SetTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeSet(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				 typ.TypeName,typ.Version.toInt)
	}
	case "TreeSetTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeTreeSet(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				     typ.TypeName,typ.Version.toInt)
	}
	case "SortedSetTypeDef" => {
	  typeDef = MdMgr.GetMdMgr.MakeSortedSet(typ.NameSpace,typ.Name,typ.TypeNameSpace,
				       typ.TypeName,typ.Version.toInt)
	}
	case "MapTypeDef" => {
	  val mapKeyType = (typ.KeyTypeNameSpace.get,typ.KeyTypeName.get)
	  val mapValueType = (typ.ValueTypeNameSpace.get,typ.ValueTypeName.get)
	  typeDef = MdMgr.GetMdMgr.MakeMap(typ.NameSpace,typ.Name,mapKeyType,mapValueType,typ.Version.toInt)
	}
	case "HashMapTypeDef" => {
	  val mapKeyType = (typ.KeyTypeNameSpace.get,typ.KeyTypeName.get)
	  val mapValueType = (typ.ValueTypeNameSpace.get,typ.ValueTypeName.get)
	  typeDef = MdMgr.GetMdMgr.MakeHashMap(typ.NameSpace,typ.Name,mapKeyType,mapValueType,typ.Version.toInt)
	}
	case "TupleTypeDef" => {
	  val tuples = typ.TupleDefinitions.get.map(arg => (arg.NameSpace,arg.Name)).toArray
	  typeDef = MdMgr.GetMdMgr.MakeTupleType(typ.NameSpace,typ.Name,tuples,typ.Version.toInt)
	}
	case "ContainerTypeDef" => {
	  if (typ.TypeName == "Struct" ){
	    typeDef = MdMgr.GetMdMgr.MakeStructDef(typ.NameSpace,typ.Name,typ.PhysicalName,
					 null,typ.Version.toInt,typ.JarName,
					 typ.DependencyJars.toArray, null, null) //BUGBUG:: Handle Keys here
	  }
	}
	case _ => {
	  throw new TypeDefProcessingException("Internal Error: Unknown Type " + typ.MetadataType)
	}
      }
      typeDef
    }catch {
      case e:AlreadyExistsException => {
	val keyValues = List(typ.NameSpace,typ.Name,typ.Version)
	val typeName = keyValues.mkString(",")
	logger.trace("Failed to add the type: " + typeName  + ": " + e.getMessage())
	throw new AlreadyExistsException(e.getMessage())
      }
      case e:Exception => {
	val keyValues = List(typ.NameSpace,typ.Name,typ.Version)
	val typeName = keyValues.mkString(",")
	logger.trace("Failed to add the type: " + typeName  + ": " + e.getMessage())
	throw new TypeDefProcessingException(e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[TypeDefListParsingException])
  def parseTypeList(typeListJson:String,formatType:String) : Array[BaseTypeDef] = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(typeListJson)

      logger.trace("Parsed the json : " + typeListJson)
      val typeList = json.extract[TypeDefList]

      logger.trace("Type count  => " + typeList.Types.length)
      val typeDefList = new Array[BaseTypeDef](typeList.Types.length)

      typeList.Types.map(typ => {
	try{
	  val typeDefObj:BaseTypeDef = processTypeDef(typ)
	  typeDefList :+ typeDefObj
	}catch {
	  case e:AlreadyExistsException => {
	    val keyValues = List(typ.NameSpace,typ.Name,typ.Version)
	    val typeName = keyValues.mkString(",")
	    logger.trace("Failed to add the type: " + typeName  + ": " + e.getMessage())
	  }
	  case e:TypeDefProcessingException => {
	    val keyValues = List(typ.NameSpace,typ.Name,typ.Version)
	    val typeName = keyValues.mkString(",")
	    logger.trace("Failed to add the type: " + typeName  + ": " + e.getMessage())
	  }
	}
      })
      typeDefList
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new TypeDefListParsingException(e.getMessage())
      }
    }
  }

  @throws(classOf[Json4sParsingException])
  @throws(classOf[ConceptListParsingException])
  def parseConceptList(conceptsStr:String,formatType:String) : Array[BaseAttributeDef] = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(conceptsStr)
      val conceptList = json.extract[ConceptList]
      val attrDefList = new Array[BaseAttributeDef](conceptList.Concepts.length)
      conceptList.Concepts.map(o => {
	try{
	  val attr = MdMgr.GetMdMgr.MakeConcept(o.NameSpace,
						o.Name,
						o.TypeNameSpace,
						o.TypeName,
						o.Version.toInt,
						false)
	  attrDefList :+ attr
	} catch {
	  case e:AlreadyExistsException => {
	    val keyValues = List(o.NameSpace,o.Name,o.Version)
	    val fullName  = keyValues.mkString(",")
	    logger.trace("Failed to add the Concept: " + fullName  + ": " + e.getMessage())
	  }
	}
      })
      attrDefList
    }catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ConceptListParsingException(e.getMessage())
      }
    }
  }

  @throws(classOf[Json4sParsingException])
  @throws(classOf[ConceptListParsingException])
  def parseDerivedConcept(conceptsStr:String,formatType:String) = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(conceptsStr)
      val concept = json.extract[DerivedConcept]
      val attrList = concept.Attributes.map(attr => (attr.NameSpace,attr.Name, attr.Type.TypeNameSpace,attr.Type.TypeName,false, attr.CollectionType.get))
      val argList = concept.FunctionDefinition.Arguments.map(arg => (arg.ArgName,arg.ArgTypeNameSpace,arg.ArgTypeName))
      val func = MdMgr.GetMdMgr.MakeFunc(concept.FunctionDefinition.NameSpace,
					 concept.FunctionDefinition.Name,
					 concept.FunctionDefinition.PhysicalName,
					 (concept.FunctionDefinition.ReturnTypeNameSpace,
					  concept.FunctionDefinition.ReturnTypeName),
					 argList,
					 null,
					 concept.FunctionDefinition.Version.toInt,
					 concept.FunctionDefinition.JarName,
					 concept.FunctionDefinition.DependantJars.toArray)

      //val derivedConcept = MdMgr.GetMdMgr.MakeDerivedAttr(func,attrList)
    }catch {
      case e:AlreadyExistsException => {
	logger.trace("Failed to add the DerivedConcept: : " + e.getMessage())
      }
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ConceptListParsingException(e.getMessage())
      }
    }
  }

  @throws(classOf[Json4sParsingException])
  @throws(classOf[ContainerDefParsingException])
  def parseContainerDef(contDefJson:String,formatType:String) : ContainerDef = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(contDefJson)

      logger.trace("Parsed the json : " + contDefJson)

      val ContDefInst = json.extract[ContainerDefinition]
      val attrList = ContDefInst.Container.Attributes.map(attr => (attr.NameSpace,attr.Name, attr.Type.TypeNameSpace,attr.Type.TypeName,false, attr.CollectionType.get))
      val contDef = MdMgr.GetMdMgr.MakeFixedContainer(ContDefInst.Container.NameSpace,
						      ContDefInst.Container.Name,
						      ContDefInst.Container.PhysicalName,
						      attrList.toList,
						      ContDefInst.Container.Version.toInt,
						      ContDefInst.Container.JarName,
						      ContDefInst.Container.DependencyJars.toArray)
      contDef
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ContainerDefParsingException(e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[TypeParsingException])
  def parseType(typeJson:String,formatType:String) : BaseTypeDef = {
    var typeDef:BaseTypeDef = null
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(typeJson)
      logger.trace("Parsed the json : " + typeJson)
      val typ = json.extract[TypeDef]
      typeDef = processTypeDef(typ)
      typeDef
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:AlreadyExistsException => {
	logger.trace("Failed to add the type, json => " + typeJson  + "\nError => " + e.getMessage())
	throw new AlreadyExistsException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new TypeParsingException(e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[ConceptParsingException])
  def parseConcept(conceptJson:String,formatType:String) : BaseAttributeDef = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(conceptJson)

      logger.trace("Parsed the json : " + conceptJson)

      val conceptInst = json.extract[Concept]
      val concept = MdMgr.GetMdMgr.MakeConcept(conceptInst.NameSpace,
					       conceptInst.Name,
					       conceptInst.TypeNameSpace,
					       conceptInst.TypeName,
					       conceptInst.Version.toInt,
					       false)
      concept
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ConceptParsingException(e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[FunctionParsingException])
  def parseFunction(functionJson:String,formatType:String) : FunctionDef = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(functionJson)

      logger.trace("Parsed the json : " + functionJson)

      val functionInst = json.extract[Function]
      val argList = functionInst.Arguments.map(arg => (arg.ArgName,arg.ArgTypeNameSpace,arg.ArgTypeName))
      val function = MdMgr.GetMdMgr.MakeFunc(functionInst.NameSpace,
					     functionInst.Name,
					     functionInst.PhysicalName,
					     (functionInst.ReturnTypeNameSpace,functionInst.ReturnTypeName),
					     argList,
					     null,
					     functionInst.Version.toInt,
					     functionInst.JarName,
					     functionInst.DependantJars.toArray)
      function
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new FunctionParsingException(e.getMessage())
      }
    }
  }


  @throws(classOf[Json4sParsingException])
  @throws(classOf[MessageDefParsingException])
  def parseMessageDef(msgDefJson:String,formatType:String) : MessageDef = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(msgDefJson)

      logger.trace("Parsed the json : " + msgDefJson)

      val MsgDefInst = json.extract[MessageDefinition]
      val attrList = MsgDefInst.Message.Attributes
      var attrList1 = List[(String, String, String,String,Boolean,String)]()
      for (attr <- attrList) {
	attrList1 ::= (attr.NameSpace,attr.Name, attr.Type.TypeNameSpace,attr.Type.TypeName,false, attr.CollectionType.get)
      }
      val msgDef = MdMgr.GetMdMgr.MakeFixedMsg(MsgDefInst.Message.NameSpace,
					       MsgDefInst.Message.Name,
					       MsgDefInst.Message.PhysicalName,
					       attrList1.toList,
					       MsgDefInst.Message.Version.toInt,
					       MsgDefInst.Message.JarName,
					       MsgDefInst.Message.DependencyJars.toArray)
      msgDef
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new MessageDefParsingException(e.getMessage())
      }
    }
  }





  @throws(classOf[Json4sParsingException])
  @throws(classOf[ModelDefParsingException])
  def parseModelDef(modDefJson:String,formatType:String) : ModelDef = {
    try{
      implicit val jsonFormats: Formats = DefaultFormats
      val json = parse(modDefJson)

      logger.trace("Parsed the json : " + modDefJson)

      val ModDefInst = json.extract[ModelDefinition]

      val inputAttrList = ModDefInst.Model.InputAttributes
      var inputAttrList1 = List[(String, String, String,String,Boolean,String)]()
      for (attr <- inputAttrList) {
	inputAttrList1 ::= (attr.NameSpace,attr.Name, attr.Type.NameSpace,attr.Type.Name,false, attr.CollectionType.get)
      }

      val outputAttrList = ModDefInst.Model.OutputAttributes
      var outputAttrList1 = List[(String, String, String)]()
      for (attr <- outputAttrList) {
	outputAttrList1 ::= (attr.Name, attr.Type.NameSpace,attr.Type.Name)
      }

      val modDef = MdMgr.GetMdMgr.MakeModelDef(ModDefInst.Model.NameSpace,
					       ModDefInst.Model.Name,
					       ModDefInst.Model.PhysicalName,
					       ModDefInst.Model.ModelType,
					       inputAttrList1,
					       outputAttrList1,
					       ModDefInst.Model.Version.toInt,
					       ModDefInst.Model.JarName,
					       ModDefInst.Model.DependencyJars.toArray)

      modDef
    } catch {
      case e:MappingException =>{
	e.printStackTrace()
	throw Json4sParsingException(e.getMessage())
      }
      case e:Exception => {
	e.printStackTrace()
	throw new ModelDefParsingException(e.getMessage())
      }
    }
  }

  @throws(classOf[UnsupportedObjectException])
  def SerializeObjectToJson(mdObj: BaseElemDef): String = {
    mdObj match{
      case o:FunctionDef => {
	val json = (("NameSpace"  -> o.nameSpace) ~
		    ("Name"       -> o.name) ~
		    ("PhysicalName"       -> o.physicalName) ~
		    ("ReturnTypeNameSpace"     -> o.retType.nameSpace) ~
		    ("ReturnTypeName"     -> o.retType.name) ~
		    ("Arguments"  -> o.args.toList.map{ arg => 
				  (
				    ("ArgName"      -> arg.name) ~
				    ("ArgTypeNameSpace"      -> arg.Type.nameSpace) ~
				    ("ArgTypeName"      -> arg.Type.name)
				  )
				  }) ~
		    ("Version"    -> o.ver) ~
		    ("JarName"    -> o.jarName) ~
		    ("DependantJars" -> o.dependencyJarNames.toList))
	pretty(render(json))
      }
      case o:MessageDef => {
	// Assume o.containerType is checked for not being null
	val json = ("Message"  -> 
		    ("NameSpace" -> o.nameSpace) ~
		    ("Name"      -> o.name) ~
		    ("FullName"  -> o.FullName) ~
		    ("Version"   -> o.ver) ~
		    ("JarName"      -> o.jarName) ~
		    ("PhysicalName" -> o.typeString) ~
		    ("DependencyJars" -> o.dependencyJarNames.toList))
	var jsonStr = pretty(render(json))
	o.containerType match{
	  case c:StructTypeDef => {
	    jsonStr = jsonStr.replaceAll("}","").trim + ",\n  \"Attributes\": "
	    var memberDefJson = SerializeObjectListToJson(o.containerType.asInstanceOf[StructTypeDef].memberDefs)
	    memberDefJson = memberDefJson + "}\n}"
	    jsonStr += memberDefJson
	    jsonStr
	  }
	  case _ => {
            throw new UnsupportedObjectException(s"SerializeObjectToJson doesn't support the " +
						 "objectType of $mdObj.name  yet")
	  }
	}
      }
      case o:ContainerDef => {
	val json = ("Container"  -> 
		    ("NameSpace" -> o.nameSpace) ~
		    ("Name"      -> o.name) ~
		    ("FullName"  -> o.FullName) ~
		    ("Version"   -> o.ver) ~
		    ("JarName"      -> o.jarName) ~
		    ("PhysicalName" -> o.typeString) ~
		    ("DependencyJars" -> o.dependencyJarNames.toList))
	var jsonStr = pretty(render(json))
	o.containerType match{
	  case c:StructTypeDef => {
	    jsonStr = jsonStr.replaceAll("}","").trim + ",\n  \"Attributes\": "
	    var memberDefJson = SerializeObjectListToJson(o.containerType.asInstanceOf[StructTypeDef].memberDefs)
	    memberDefJson = memberDefJson + "}\n}"
	    jsonStr += memberDefJson
	    jsonStr
	  }
	  case _ => {
            throw new UnsupportedObjectException(s"SerializeObjectToJson doesn't support the " +
						 "objectType of $mdObj.name  yet")
	  }
	}
      }
      case o:ModelDef => {
	val json = ( "Model" -> ("NameSpace" -> o.nameSpace) ~
		    ("Name" -> o.name) ~
		    ("Version" -> o.ver) ~
		    ("ModelType"  -> o.modelType) ~
		    ("JarName" -> o.jarName) ~
		    ("PhysicalName" -> o.typeString) ~
		    ("DependencyJars" -> o.dependencyJarNames.toList))
	var jsonStr = pretty(render(json))
	jsonStr = jsonStr.replaceAll("}","").trim
	jsonStr = jsonStr + ",\n\"InputAttributes\": "
	var memberDefJson = SerializeObjectListToJson(o.inputVars)
	jsonStr += memberDefJson

	jsonStr = jsonStr + ",\n\"OutputAttributes\": "
	memberDefJson = SerializeObjectListToJson(o.outputVars)
	memberDefJson = memberDefJson + "}\n}"
	jsonStr += memberDefJson
	jsonStr
      }

      case o:AttributeDef => {
	val json = (("NameSpace" -> o.name) ~
		    ("Name" -> o.name) ~
		    ("Version" -> o.ver) ~
		    ("CollectionType" -> ObjType.asString(o.collectionType)))
	var jsonStr = pretty(render(json))
	jsonStr = jsonStr.replaceAll("}","").trim + ",\n  \"Type\": "
	var memberDefJson = SerializeObjectToJson(o.typeDef)
	memberDefJson = memberDefJson + "}"
	jsonStr += memberDefJson
	jsonStr
      }
      case o:ScalarTypeDef => {
	val json =  (("MetadataType" -> "ScalarTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> MdMgr.sysNS ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName))
	pretty(render(json))
      }
      case o:SetTypeDef => {
	val json =  (("MetadataType" -> "SetTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("KeyTypeNameSpace" -> o.keyDef.nameSpace ) ~
		     ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)))

	pretty(render(json))
      }
      case o:TreeSetTypeDef => {
	val json =  (("MetadataType" -> "TreeSetTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("KeyTypeNameSpace" -> o.keyDef.nameSpace ) ~
		     ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)))

	pretty(render(json))
      }
      case o:SortedSetTypeDef => {
	val json =  (("MetadataType" -> "SortedSetTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("KeyTypeNameSpace" -> o.keyDef.nameSpace ) ~
		     ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)))

	pretty(render(json))
      }
      case o:MapTypeDef => {
	val json =  (("MetadataType" -> "MapTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("KeyTypeNameSpace" -> o.keyDef.nameSpace ) ~
		     ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)) ~
		     ("ValueTypeNameSpace" -> o.valDef.nameSpace ) ~
		     ("ValueTypeName" -> ObjType.asString(o.valDef.tType)))
	pretty(render(json))
      }
      case o:HashMapTypeDef => {
	val json =  (("MetadataType" -> "HashMapTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("KeyTypeNameSpace" -> o.keyDef.nameSpace ) ~
		     ("KeyTypeName" -> ObjType.asString(o.keyDef.tType)) ~
		     ("ValueTypeNameSpace" -> o.valDef.nameSpace ) ~
		     ("ValueTypeName" -> ObjType.asString(o.valDef.tType)))
	pretty(render(json))
      }
      case o:ListTypeDef => {
	val json =  (("MetadataType" -> "ListTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("ValueTypeNameSpace" -> o.valDef.nameSpace ) ~
		     ("ValueTypeName" -> ObjType.asString(o.valDef.tType)))
	pretty(render(json))
      }
      case o:QueueTypeDef => {
	val json =  (("MetadataType" -> "QueueTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("Fixed" -> o.IsFixed) ~
		     ("ValueTypeNameSpace" -> o.valDef.nameSpace ) ~
		     ("ValueTypeName" -> ObjType.asString(o.valDef.tType)))
	pretty(render(json))
      }
      case o:ArrayTypeDef => {
	val json =  (("MetadataType" -> "ArrayTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.elemDef.tTypeType) ) ~
		     ("TypeNameSpace" -> o.elemDef.nameSpace ) ~
		     ("TypeName" -> ObjType.asString(o.elemDef.tType) ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("NumberOfDimensions" -> o.arrayDims))
	pretty(render(json))
      }
      case o:ArrayBufTypeDef => {
	val json =  (("MetadataType" -> "ArrayBufTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.elemDef.tTypeType) ) ~
		     ("TypeNameSpace" -> o.elemDef.nameSpace ) ~
		     ("TypeName" -> ObjType.asString(o.elemDef.tType) ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) ~
		     ("NumberOfDimensions" -> o.arrayDims))
	pretty(render(json))
      }
      case o:TupleTypeDef => {
	var json =  (("MetadataType" -> "TupleTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> MdMgr.sysNS ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName) )
	var jsonStr = pretty(render(json))
	jsonStr = jsonStr.replaceAll("}","").trim + ",\n  \"TupleDefinitions\": "
	var tupleDefJson = SerializeObjectListToJson(o.tupleDefs)
	tupleDefJson = tupleDefJson + "}"
	jsonStr += tupleDefJson
	jsonStr
      }
      case o:ContainerTypeDef => {
	val json =  (("MetadataType" -> "ContainerTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> MdMgr.sysNS ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName))
	pretty(render(json))
      }
      case o:AnyTypeDef => {
	val json =  (("MetadataType" -> "AnyTypeDef") ~
		     ("NameSpace" -> o.nameSpace) ~
		     ("Name" -> o.name) ~
		     ("TypeTypeName" -> ObjTypeType.asString(o.tTypeType) ) ~
		     ("TypeNameSpace" -> o.nameSpace ) ~
		     ("TypeName" -> o.name ) ~
		     ("PhysicalName" -> o.physicalName ) ~
		     ("Version" -> o.ver) ~
		     ("JarName" -> o.jarName) ~
		     ("DependencyJars" -> getJarList(o.dependencyJarNames)) ~
		     ("Implementation" -> o.implementationName))
	pretty(render(json))
      }
      case _ => {
        throw new UnsupportedObjectException(s"SerializeObjectToJson doesn't support the objectType of $mdObj.name  yet")
      }
    }
  }

  def getJarList(jarArray: Array[String]): List[String] = {
    if (jarArray != null ){
      jarArray.toList
    }
    else{
      new Array[String](0).toList
    }
  }


  def SerializeObjectListToJson[T <: BaseElemDef](objList: Array[T]) : String = {
    var json = "[ \n" 
    objList.toList.map(obj =>  {var objJson = SerializeObjectToJson(obj); json += objJson; json += ",\n"})
    json = json.stripSuffix(",\n")
    json += " ]\n"
    json 
  }

  def SerializeObjectListToJson[T <: BaseElemDef](objType:String, objList: Array[T]) : String = {
    var json = "{\n" + "\"" + objType + "\" :" + SerializeObjectListToJson(objList) + "\n}" 
    json 
  }

}
