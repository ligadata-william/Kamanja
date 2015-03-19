package com.ligadata.Serialize

import java.util.Properties
import java.io._
import scala.Enumeration
import scala.io.Source._
import org.apache.log4j._

import scala.collection.JavaConversions._

import com.ligadata.olep.metadata._

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.ExtensionRegistry;

import com.ligadata.Serialize.MetadataObjects._
import com.ligadata.Serialize.MetadataObjects.MetadataType._
import java.io._

case class ProtoBufSerializationException(e: String) extends Throwable(e)

class ProtoBufSerializer extends Serializer{

  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
  private[this] var classLoader: java.lang.ClassLoader = null

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }

  def buildProtoBaseElem(o: BaseElemDef) : ProtoBaseElem = {
    logger.trace("Build ProtoBaseElem from " + o.getClass().getName())
    val pbe = ProtoBaseElem.newBuilder()
      .setNameSpace(o.NameSpace)
      .setName(o.Name)
      .setVer(o.Version)
    if( o.PhysicalName != null ){
      pbe.setPhysicalName(o.PhysicalName)
    }
    if( o.JarName != null ){
      pbe.setJarName(o.JarName)
    }
    if( o.DependencyJarNames.length > 0 ){
      o.DependencyJarNames.toList.map(obj => {pbe.addDependencyJarNames(obj)})
    }
    pbe.build
  }

  def buildProtoBaseType(o: BaseTypeDef) : ProtoBaseType = {
    logger.trace("Build ProtoBaseType from " + o.getClass().getName())
    val pbt = ProtoBaseType.newBuilder()
      .setPbe(buildProtoBaseElem(o))
   //   .setTypeType(ObjTypeType.asString(o.tTypeType))
   //   .setType(ObjType.asString(o.tType))

     if(o.implementationName != null ){
      pbt.setImplementationName(o.implementationName)
     }
     pbt.build()
  }

  def buildProtoBaseTypeKey(o: BaseTypeDef) : ProtoBaseTypeKey = {
    logger.trace("Build ProtoBaseTypekey from " + o.getClass().getName())
    val pbtk = ProtoBaseTypeKey.newBuilder()
      .setNameSpace(o.NameSpace)
      .setName(o.Name)
      .setVer(o.Version)
     pbtk.build()
  }

  def buildAttribute(o: AttributeDef) : Attribute = {
    val a = 
      Attribute.newBuilder()
      .setPbe(buildProtoBaseElem(o))
      .setPbt(buildProtoBaseTypeKey(o.aType))

    if(o.collectionType != ObjType.tNone ){
      a.setCollectionType(ObjType.asString(o.collectionType))
    }
    a.build()
  }

  def buildModel(o: ModelDef) : Model = {
    val m = Model.newBuilder()
    
    m.setPbe(buildProtoBaseElem(o))
    m.setModelType("RuleSet")
    o.inputVars.toList.map(obj => {m.addInputVars(buildAttribute(obj.asInstanceOf[AttributeDef]))})
    o.outputVars.toList.map(obj => {m.addOutputVars(buildAttribute(obj.asInstanceOf[AttributeDef]))})
    m.build()
  }


  def buildModel(o: Model) : ModelDef = {

    try{
      /** 
       *  FIXME: This is broken.  The model.inputVars and model.outputVars
       *  are no longer represented as arrays of attributes.  Their elements
       *  are now instances of ModelInputVariable and ModelOutputVariable respectively.
       *  
       *  Some new code is needed for the MetadataObjects.  I don't understand
       *  protobuf well enough at this point to fix that.  Since we are using 
       *  kryo, this is work left to do (OR we nuke this implementation).
       *  
       */
      
      val inputAttrList  = o.getInputVarsList()

      logger.trace("Prepping InputVarList...")

      var inputAttrList1 = List[(String, String, String,String,Boolean,String)]()
      for (attr <- inputAttrList) {
	var collType:String = null;
	if( attr.getCollectionType() != null ){
	  collType = attr.getCollectionType();
	}
	inputAttrList1 ::= (attr.getPbe().getNameSpace(),
			    attr.getPbe().getName(),
			    attr.getPbt().getNameSpace(),
			    attr.getPbt().getName(),
			    false, 
			    collType)
      }

      logger.trace("Prepping OutputVarList...")

      val outputAttrList = o.getOutputVarsList()
      var outputAttrList1 = List[(String, String, String)]()
      for (attr <- outputAttrList) {
	outputAttrList1 ::= (attr.getPbe().getName(),
			     attr.getPbt().getNameSpace(),
			     attr.getPbt().getName())
      }


      logger.trace("Prepping DependencyJars...")
      // Couldn't use toArray on List[java.lang.String], need to make it better
      val depJarsList = o.getPbe().getDependencyJarNamesList()
      val depJars1 = new Array[String](depJarsList.length)
      var i:Int = 0
      for( str <- depJarsList){
	depJars1(i) = str
	i += 1
      }

      //logger.trace("depJars contain " + depJars1.length + " entries ")
      //depJars1.foreach(s => { logger.trace(s) })

      logger.trace("Create the ModelDef object...")
      val m = MdMgr.GetMdMgr.MakeModelDef(o.getPbe().getNameSpace(),
					  o.getPbe().getName(),
					  o.getPbe().getPhysicalName(),
					  o.getModelType(),
					  null, //inputAttrList1,
					  null, //outputAttrList1,
					  o.getPbe().getVer(),
					  o.getPbe().getJarName(),
					  depJars1)
      
      m
    }catch{
      case e:Exception => {
	e.printStackTrace()
	throw new ProtoBufSerializationException("Failed to Deserialize the object: " + e.getMessage())
      }
    }
  }


  override def SerializeObjectToByteArray(obj : Object) : Array[Byte] = {
    try{
      obj match {
	case o:AttributeDef => {
	  val ae = buildAttribute(o)
	  val ba = ae.toByteArray();
	  logger.trace("Serialized data contains " + ba.length + " bytes ")
	  ba
	}
	case o:ModelDef => {
	  val m = buildModel(o)
	  val ba = m.toByteArray();
	  logger.trace("Serialized data contains " + ba.length + " bytes ")
	  ba
	}
      }
    }catch{
      case e:Exception => {
	throw new ProtoBufSerializationException("Failed to Serialize the object(" + obj.getClass().getName() + "): " + e.getMessage())
      }
    }
  }


  override def DeserializeObjectFromByteArray(ba: Array[Byte], objectType:String) : Object = {
    logger.trace("Parse " + ba.length + " bytes to create a " + objectType + " object ")
    try{
      objectType match{
	case "com.ligadata.olep.metadata.AttributeDef" => {
	  val a = Attribute.parseFrom(ba);
	  logger.trace("Attribute => " + a)
	  a
	}
	case "com.ligadata.olep.metadata.ModelDef" => {
	  val m = Model.parseFrom(ba);
	  logger.trace("Deserialized Model as protobuf object => " + m)
	  val mDef = buildModel(m)
	  mDef
	}
        case _ => {
	  throw new ProtoBufSerializationException("Failed to DeSerialize the object of type: " + objectType)
	}
      }
    }catch{
      case e:Exception => {
	throw new ProtoBufSerializationException("Failed to DeSerialize the object:" + e.getMessage())
      }
    }
  }


  override def DeserializeObjectFromByteArray(ba: Array[Byte]) : Object = {
    throw new ProtoBufSerializationException("Failed to DeSerialize the object: Unable to deserialize the object without ObjectType")
  }

  override def SetClassLoader(cl : java.lang.ClassLoader): Unit = {
    classLoader = cl
  }

}
