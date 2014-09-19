package com.ligadata.olep.metadata

import scala.Enumeration
import scala.collection.immutable.List
import scala.collection.mutable.{ Map, HashMap, MultiMap, Set, SortedSet, ArrayBuffer }
import scala.io.Source._
import scala.util.control.Breaks._

import java.util._
import java.lang.RuntimeException
import java.util.NoSuchElementException

import org.apache.log4j._

import ObjTypeType._
import ObjType._




case class AlreadyExistsException(e: String) extends Throwable(e)
case class ObjectNolongerExistsException(e: String) extends Throwable(e)

/**
 * class MdMgr
 *
 * Accessor functions are provided to access various metadata entities.  These entities include:
 *
 * MessageDef, TypeDef, FunctionDef, ConceptDef/AttributeDef, DerivedConceptDef/ComputedAttributeDef, ModelDef
 *
 * Each metadata entity could be uniquely identified by a key (which includes version number).
 * Each metadata entity could be stored persistently in the underlying persistent store (either key-val or relational schema)
 *
 * There is one container for each type of metadata entity with the exception of FunctionDefs which has two representations:
 * funcDefs uses a string key consisting in the form "fcnNmSpc.functionName(argNmSpc.argType, argNmSpc.argType,...)"
 * funcDefSets uses a simple name key in the form "fcnNmSpc:functionName"
 * The PmmlCompiler is the principal user of the funcDefs, requiring the necessary disambiguation of types with the same base name
 * via the argument qualification.  The funcDefSets will aid the authoring tools to discover what variations of a given named
 * function are cataloged in the metadata.
 *
 */

//BUGBUG:: ????????????????? Not yet allowing more than one version at this moment ????????????????? 
class MdMgr {

  /** initialize a logger */
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)

  /** maps that hold caches of the metadata */
  /** making every thing is multi map, because we will have multiple versions for the same one */
  private var typeDefs = new HashMap[String, Set[BaseTypeDef]] with MultiMap[String, BaseTypeDef]
  private var funcDefs = new HashMap[String, Set[FunctionDef]] with MultiMap[String, FunctionDef]
  private var msgDefs = new HashMap[String, Set[MessageDef]] with MultiMap[String, MessageDef]
  private var containerDefs = new HashMap[String, Set[ContainerDef]] with MultiMap[String, ContainerDef]
  private var attrbDefs = new HashMap[String, Set[BaseAttributeDef]] with MultiMap[String, BaseAttributeDef]
  private var modelDefs = new HashMap[String, Set[ModelDef]] with MultiMap[String, ModelDef]

  // FunctionDefs keyed by function signature nmspc.name(argtyp1,argtyp2,...) map 
  private var compilerFuncDefs = scala.collection.mutable.Map[String, FunctionDef]()
  // Function style macros used by the Pmml Compiler to support code generation. macroDef key is typesig. 
  private var macroDefs = scala.collection.mutable.Map[String, MacroDef]()
  private var macroDefSets = new HashMap[String, Set[MacroDef]] with MultiMap[String, MacroDef]

  def SetLoggerLevel(level: Level){
    logger.setLevel(level);
  }

  def truncate{
    typeDefs.clear
    funcDefs.clear
    msgDefs.clear
    containerDefs.clear
    attrbDefs.clear
    modelDefs.clear
    compilerFuncDefs.clear
    macroDefs.clear
    macroDefSets.clear
  }


  def truncate(objectType:String){
    objectType match{
      case "TypeDef" => {
	typeDefs.clear
      }
      case "FunctionDef" => {
	funcDefs.clear
	compilerFuncDefs.clear
      }
      case "MessageDef" => {
	msgDefs.clear
      }
      case "ContainerDef" => {
	containerDefs.clear
      }
      case "ConceptDef" => {
	attrbDefs.clear
      }
      case "ModelDef" => {
	modelDefs.clear
      }
      case "CompilerFuncDef" => {
	compilerFuncDefs.clear
      }
      case "MacroDef" => {
	macroDefs.clear
      }
      case "MacroDefSets" => {
	macroDefSets.clear
      }
      case _ => {
	logger.error("Unknown object type " + objectType + " in truncate function")
      }
    }
  }


  def dump{
    typeDefs.foreach(obj  => {logger.trace("Type Key = " + obj._1)})
    funcDefs.foreach(obj  => {logger.trace("Function Key = " + obj._1)})
    msgDefs.foreach(obj   => {logger.trace("Message Key = " + obj._1)})
    containerDefs.foreach(obj  => {logger.trace("Container Key = " + obj._1)})
    attrbDefs.foreach(obj  => {logger.trace("Attribute Key = " + obj._1)})
    modelDefs.foreach(obj  => {logger.trace("Model Key = " + obj._1)})
    compilerFuncDefs.foreach(obj  => {logger.trace("CompilerFunction Key = " + obj._1)})
    macroDefs.foreach(obj  => {logger.trace("Macro Key = " + obj._1)})
    macroDefSets.foreach(obj  => {logger.trace("MacroSet Key = " + obj._1)})
  }

  private def GetExactVersion[T <: BaseElemDef](elems: Option[scala.collection.immutable.Set[T]], ver: Int): Option[T] = {
    // get exact match
    elems match {
      case None => None
      case Some(es) => {
        var elm: Option[T] = None
        try {
          breakable {
            // We can use this, but it will loop thru all elements 
            // elems.filter(e => e.Version == ver)
            es.foreach(e => {
              if (e.Version == ver) {
                elm = Some(e)
                break
              }
            })
          }
        } catch {
          case e: Exception => {}
        }
        elm
      }
    }
  }

  private def GetLatestVersion[T <: BaseElemDef](elems: Option[scala.collection.immutable.Set[T]]): Option[T] = {
    // get latest one
    elems match {
      case None => None
      case Some(es) => {
        var elm: Option[T] = None
        var maxVer: Int = -1
        try {
          breakable {
            es.foreach(e => {
              if (elm == None || maxVer < e.Version) {
                elm = Some(e)
                maxVer = e.Version
              }
            })
          }
        } catch {
          case e: Exception => {}
        }
        elm
      }
    }
  }

  /** Get Matched Value */
  private def GetReqValue[T <: BaseElemDef](elems: Option[scala.collection.immutable.Set[T]], ver: Int): Option[T] = {
    if (ver <= 0) GetLatestVersion(elems) else GetExactVersion(elems, ver)
  }

  /** Get Immutable Set from Mutable Set */
  private def GetImmutableSet[T <: BaseElemDef](elems: Option[scala.collection.mutable.Iterable[T]], onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[T]] = {
    if (latestVersion == false) {
      elems match {
        case None => None
        case Some(es) => { 
	  if (onlyActive) Some(es.filter(e => e.IsActive).toSet) else Some(es.toSet)
	}
      }
    } else {
      val latestElems =
        // get latest ones
        elems match {
          case None => None
          case Some(es) => {
            var newElems = new scala.collection.mutable.ArrayBuffer[T]()
            var newBaseElemsIdxs = scala.collection.mutable.Map[String, Int]()
            es.foreach(e => {
              if (onlyActive == false || (onlyActive && e.IsActive)) {
                val fnm = if (e.isInstanceOf[FunctionDef]) e.asInstanceOf[FunctionDef].typeString else e.FullName
                val existingIdx = newBaseElemsIdxs.getOrElse(fnm, -1)
                if (existingIdx < 0) {
                  newBaseElemsIdxs(fnm) = newElems.size
                  newElems += e
                } else if (newElems(existingIdx).Version < e.Version) {
                  newElems(existingIdx) = e
                }
              }
            })
            Some(newElems.toSet)
          }
        }
      latestElems
    }
  }

  private def GetElem[T <: BaseElemDef](elm: Option[T], noSuchElemErr: String): T = {
    elm match {
      case None => throw new NoSuchElementException(noSuchElemErr)
      case Some(e) => elm.get
    }
  }

  /**
   *  Fill in the BaseElement info common to all types.
   *
   *  @param be - the BaseElemDef
   *  @param nameSpace - the scalar type namespace
   *  @param name - the scalar type name.
   *  @param ver - version info

   *  @param jarNm - 
   *  @param depJars - 
   */

  private def SetBaseElem(be: BaseElemDef, nameSpace: String, name: String, ver: Int, jarNm: String, depJars: Array[String]): Unit = {
    be.name = name.trim.toLowerCase
    be.nameSpace = nameSpace.trim.toLowerCase
    be.ver = ver
    be.uniqueId = MdIdSeq.next
    be.creationTime = System.currentTimeMillis // Taking current local time. May be we need to get GMT time
    be.modTime = be.creationTime
    be.jarName = jarNm
    be.dependencyJarNames = depJars
  }

  /**
   *  Construct an AttributeDef from the supplied arguments.
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the name of the structure type.
   *  @param typeNameNs - the namespace of the type for this attribute's type
   *  @param typeName - the name of the attribute's type
   *  @param ver - version info
   *  @param findInGlobal - attribute is a explicitly cataloged concept?
   *  @return an AttributeDef
   *
   */

  @throws(classOf[NoSuchElementException])
  private def MakeAttribDef(nameSpace: String, name: String, typeNameNs: String, typeName: String, ver: Int, findInGlobal: Boolean): BaseAttributeDef = {
    if (findInGlobal) {
      val atr = GetElem(Attribute(nameSpace, name, -1, false), s"Attribute $nameSpace.$name does not exist")
      if (atr == null) { throw new NoSuchElementException(s"Attribute $nameSpace.$name does not exist") }
      return atr
    }

    var ad = new AttributeDef
    ad.inherited = null
    ad.aType = GetElem(Type(typeNameNs, typeName, -1, false), s"Type $typeNameNs.$typeName does not exist")
    if (ad.aType == null) { throw new NoSuchElementException(s"Type $typeNameNs.$typeName does not exist") }

    val depJarSet = scala.collection.mutable.Set[String]()
    if (ad.aType.JarName != null) depJarSet += ad.aType.JarName
    if (ad.aType.DependencyJarNames != null) depJarSet ++= ad.aType.DependencyJarNames
    val dJars = if (depJarSet.size > 0) depJarSet.toArray else null

    SetBaseElem(ad, nameSpace, name, ver, null, dJars)
    ad
  }

  
  /**
   *  MakeContainerTypeMap participates in the construction of mapped based messages, providing the base type container.
   *
   *  @param nameSpace - the container type namespace
   *  @param name - the container name.
   *  @param args - a List of triples (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   *  @param ver - version info

   *  @param jarNm - 
   *  @param depJars - 
   *  @return an MappedMsgTypeDef
   *
   */

  @throws(classOf[NoSuchElementException])
  private def MakeContainerTypeMap(nameSpace: String, name: String, physicalName: String, args: List[(String, String, String, String, Boolean)], ver: Int, jarNm: String, depJars: Array[String]): MappedMsgTypeDef = {
    val st = new MappedMsgTypeDef
    val depJarSet = scala.collection.mutable.Set[String]()

    val msgNm = MdMgr.MkFullName(nameSpace, name)

    args.foreach(elem => {
      val (nsp, nm, typnsp, typenm, isGlobal) = elem
      val nmSp = if (nsp != null) nsp else msgNm //BUGBUG:: when nsp != do we need to check for isGlobal is true?????
      val attr = MakeAttribDef(nmSp, nm, typnsp, typenm, ver, isGlobal)
      if (attr.JarName != null) depJarSet += attr.JarName
      if (attr.DependencyJarNames != null) depJarSet ++= attr.DependencyJarNames
      st.attrMap(elem._1) = attr
    })

    if (depJars != null) depJarSet ++= depJars
    val dJars = if (depJarSet.size > 0) depJarSet.toArray else null
    SetBaseElem(st, nameSpace, name, ver, jarNm, dJars)
    st.PhysicalName(physicalName)
    st
  }

  /**
   *  Construct a StructTypeDef used as the fixed message core type.
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the name of the structure type.
   *  @param args - a List of triples (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   *  @param ver - version info

   *  @param jarNm - 
   *  @param depJars - 
   *  @return the constructed StructTypeDef
   */

  def MakeStructDef(nameSpace: String, name: String, physicalName: String, args: List[(String, String, String, String, Boolean)], ver: Int, jarNm: String, depJars: Array[String]): StructTypeDef = {
    var sd = new StructTypeDef

    val msgNm = MdMgr.MkFullName(nameSpace, name)

    val depJarSet = scala.collection.mutable.Set[String]()

    sd.memberDefs = args.map(elem => {
      val (nsp, nm, typnsp, typenm, isGlobal) = elem
      val nmSp = if (nsp != null) nsp else msgNm //BUGBUG:: when nsp != do we need to check for isGlobal is true?????
      val atr = MakeAttribDef(nmSp, nm, typnsp, typenm, ver, isGlobal)
      if (atr.JarName != null) depJarSet += atr.JarName
      if (atr.DependencyJarNames != null) depJarSet ++= atr.DependencyJarNames
      atr
    }).toArray

    if (depJars != null) depJarSet ++= depJars
    val dJars = if (depJarSet.size > 0) depJarSet.toArray else null

    SetBaseElem(sd, nameSpace, name, ver, jarNm, dJars)
    sd.PhysicalName(physicalName)

    sd
  }

  /**
   *  Construct an ArgDef from the supplied arguments.
   *
   *  @param fn - the FunctionDef instance to receive the constructed argument
   *  @param name - the arguments parameter name.
   *  @param nmSpc - the type namespace for this argument
   *  @param tName - the argument's type name
   *  @return an ArgDef
   *
   */

  @throws(classOf[NoSuchElementException])
  private def MakeArgDef(name: String, nmSpc: String, tName: String): ArgDef = {
    val aType = GetElem(Type(nmSpc, tName, -1, false), s"Argument type $nmSpc.$tName does not exist")
    if (aType == null) { throw new NoSuchElementException(s"Argument type $nmSpc.$tName does not exist") }
    val ad = new ArgDef()
    ad.name = name
    ad.aType = aType
    ad
  }

  // External Functions -- Start 

  // Get Functions
  /** Get All Versions of Types */
  def Types(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseTypeDef]] = { GetImmutableSet(Some(typeDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /** Get All Versions of Types for Key */
  def Types(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseTypeDef]] = { GetImmutableSet(typeDefs.get(key.trim.toLowerCase), onlyActive, latestVersion) }
  def Types(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseTypeDef]] = Types(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)

  /** Answer the BaseTypeDef with the supplied namespace and name  */
  def Type(nameSpace: String, name: String, ver: Int, onlyActive: Boolean): Option[BaseTypeDef] = Type(MdMgr.MkFullName(nameSpace, name), ver, onlyActive)

  /** Answer the Active AND Current BaseTypeDef with the supplied namespace and name  */
  def ActiveType(nameSpace: String, name: String): BaseTypeDef = {
	  val optContainer : Option[BaseTypeDef] = Type(MdMgr.MkFullName(nameSpace, name), -1, true)
	  val container : BaseTypeDef = optContainer match {
	    case Some(optContainer) => optContainer
	    case _ => null
	  }
	  container
  }

  /** Answer the BaseTypeDef with the supplied key  */
  def Type(key: String, ver: Int, onlyActive: Boolean): Option[BaseTypeDef] = GetReqValue(Types(key, onlyActive, false), ver)
  /** Answer the BaseTypeDef with the supplied key  */
  def ActiveType(key: String): BaseTypeDef = {
	  val typ : Option[BaseTypeDef] = GetReqValue(Types(key.toLowerCase(), true, false), -1)
	  val activeType : BaseTypeDef = typ match {
	    case Some(typ) => typ
	    case _ => null
	  }
	  activeType
  }

  /** Get All Versions of Messages */
  def Messages(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[MessageDef]] = { GetImmutableSet(Some(msgDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /** Get All Versions of Messages for Key */
  def Messages(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[MessageDef]] = { GetImmutableSet(msgDefs.get(key.trim.toLowerCase), onlyActive, latestVersion) }
  def Messages(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[MessageDef]] = Messages(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)

  /** Answer the MessageDef with the supplied namespace and name  */
  def Message(nameSpace: String, name: String, ver: Int, onlyActive: Boolean): Option[MessageDef] = Message(MdMgr.MkFullName(nameSpace, name), ver, onlyActive)
  /** Answer the ACTIVE and CURRENT MessageDef with the supplied namespace and name  */
  def ActiveMessage(nameSpace: String, name: String): MessageDef = {
	  val optMsg : Option[MessageDef] = Message(MdMgr.MkFullName(nameSpace, name), -1, true)
	  val msg : MessageDef = optMsg match {
	    case Some(optMsg) => optMsg
	    case _ => null
	  }
	  msg
  }

  /** Answer the MessageDef with the supplied key. */
  def Message(key: String, ver: Int, onlyActive: Boolean): Option[MessageDef] = GetReqValue(Messages(key, onlyActive, false), ver)

  /** Get All Versions of Containers for Key */
  def Containers(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ContainerDef]] = { GetImmutableSet(Some(containerDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /** Get All Versions of Containers for Key */
  def Containers(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ContainerDef]] = { GetImmutableSet(containerDefs.get(key.trim.toLowerCase), onlyActive, latestVersion) }
  def Containers(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ContainerDef]] = Containers(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)

  /** Answer the ContainerDef with the supplied namespace and name  */
  def Container(nameSpace: String, name: String, ver: Int, onlyActive: Boolean): Option[ContainerDef] = Container(MdMgr.MkFullName(nameSpace, name), ver, onlyActive)

  /** Answer the Active AND Current ContainerDef with the supplied namespace and name  */
  def ActiveContainer(nameSpace: String, name: String): ContainerDef = {
	  val optContainer : Option[ContainerDef] = Container(MdMgr.MkFullName(nameSpace, name), -1, true)
	  val container : ContainerDef = optContainer match {
	    case Some(optContainer) => optContainer
	    case _ => null
	  }
	  container
  }

  /** Answer the ContainerDef with the supplied key.  Use one of the helper functions described here to form a proper search key.  */
  def Container(key: String, ver: Int, onlyActive: Boolean): Option[ContainerDef] = GetReqValue(Containers(key, onlyActive, false), ver)

  /** Get All Versions of Functions */
  def Functions(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[FunctionDef]] = { GetImmutableSet(Some(funcDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /**
   * If latestVersion is true it will get only Latest versions of unique function signature (only by positional at this moment, not yet handling named arguments)
   *  If latestVersion is false it will get all versions of function name matches to the key
   */
  def Functions(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[FunctionDef]] = Functions(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)
  def Functions(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[FunctionDef]] = {
    val allfns = funcDefs.get(key)
    if (latestVersion == false)
      return GetImmutableSet(allfns, onlyActive, latestVersion)

    val latestFns =
      // get latest one
      allfns match {
        case None => None
        case Some(fs) => {
          var newFns = scala.collection.mutable.Map[String, FunctionDef]()
          fs.foreach(f => {
            if (onlyActive == false || (onlyActive && f.IsActive)) {
              val fnm = f.FullName // this returns like system.fn1(int, int)
              val existingFn = newFns.getOrElse(fnm, null)
              if (existingFn == null || existingFn.Version < f.Version)
                newFns(fnm) = f
            }
          })
          Some(newFns.map(_._2).toSet)
        }
      }

    latestFns
  }
  
  /** Answer the ACTIVE and CURRENT function definitions for the supplied namespace.name */
  def FunctionsAvailable(nameSpace: String, name: String): scala.collection.immutable.Set[FunctionDef] = {
	  val optFcns : Option[scala.collection.immutable.Set[FunctionDef]]  = Functions(nameSpace.toLowerCase(), name.toLowerCase(), true, true)
	  val fcns : scala.collection.immutable.Set[FunctionDef] = optFcns match {
	    case Some(optFcns) => optFcns
	    case _ => null
	  }
	  fcns
  }

  /* full key name and full arguments names */
  def Function(key: String, args: List[String], ver: Int, onlyActive: Boolean): Option[FunctionDef] = {
    // get functions which match to key & arguments
    val fnMatches =
      funcDefs.get(key) match {
        case None => None
        case Some(fs) => {
          // val signature = key.trim.toLowerCase + "(" + args.foldLeft("")((sig, elem) => sig + "," + elem.trim.toLowerCase) + ")"
          val signature = key.trim.toLowerCase + "(" + args.map(elem => elem.trim.toLowerCase).mkString(",") + ")"
	  //logger.trace("signature => " + signature)
	  //logger.trace("fs => " + fs.size)
          val matches = fs.filter(f => (onlyActive == false || (onlyActive && f.IsActive)) && (signature == f.typeString))
	  //logger.trace("matches => " + matches.toSet.size)
          if (matches.size > 0) Some(matches.toSet) else None
        }
      }

    GetReqValue(fnMatches, ver)
  }

  /* namespace & name for key and fullname for each arg */
  // def Function(nameSpace: String, name: String, args: List[String], ver: Int): Option[FunctionDef] = Function(MkFullName(nameSpace, name), args, ver)

  /* namespace & name for key and namespace & name for each arg */
  def Function(nameSpace: String, name: String, args: List[(String, String)], ver: Int, onlyActive: Boolean): Option[FunctionDef] = Function(MdMgr.MkFullName(nameSpace, name), args.map(a => MdMgr.MkFullName(a._1, a._2)), ver, onlyActive)

  /**
   *  Principally used by the Pmml Compiler, answer the FunctionDef with the supplied type signature.  
   *  Type signatures are in the following form:
   *  
   *  		namespace.functionName(arg.typeString,arg.typeString,...)
   *    
   *  NOTE: Typestrings are the native scala type representation, not the Ligadata namespace.typename.  For example,
   *  
   *  		Udfs.ContainerMap(Array[SomeMessageType],String,Int,Int,Boolean)
   *    
   *  @param key : a typestring key
   *  @return a FunctionDef matching the type signature key or null if there is no match
   */   
  def FunctionByTypeSig(key : String): FunctionDef = { compilerFuncDefs.get(key.toLowerCase()).getOrElse(null) }
  
  /**
   *  Principally used by the Pmml Compiler, answer the macro that matches the supplied type signature.  A 
   *  MacroDef is returned.  The MacroDef is similar in most respects to the FunctionDef (in fact a 
   *  MacroDef isA FunctionDef).  The difference is how they are used by the Pmml compiler.  
   *  @see <documentation reference> for more details regarding function macros.  
   *  @see FunctionByTypeSig for details regarding key format.
   *    
   *  @param key : a typestring key
   *  @return a MacroDef matching the type signature key or null if there is no match
   */ 
  def MacroByTypeSig(key : String): MacroDef = { macroDefs.get(key.toLowerCase()).getOrElse(null) }
  /** 
   *  Answer the function macros that match the supplied full name.  See MdMgr.MkFullName for key format.
   *  This method is principally for the author and the authoring tools to investigate what is available for
   *  use during construction or rule sets and other models.
   *    
   *  @param key : a typestring key
   *  @return a FunctionDef or null if there is no match
   */ 
  def MacrosAvailable(key : String): Set[MacroDef] = { macroDefSets.get(key.toLowerCase()).getOrElse(Set[MacroDef]()) }

  /** Get All Versions of Attributes */
  def Attributes(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseAttributeDef]] = { GetImmutableSet(Some(attrbDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /** Get All Versions of Attributes for Key */
  def Attributes(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseAttributeDef]] = { GetImmutableSet(attrbDefs.get(key.trim.toLowerCase), onlyActive, latestVersion) }
  def Attributes(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[BaseAttributeDef]] = Attributes(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)

  /** Answer the BaseAttributeDef with the supplied namespace and name  */
  def Attribute(nameSpace: String, name: String, ver: Int, onlyActive: Boolean): Option[BaseAttributeDef] = Attribute(MdMgr.MkFullName(nameSpace, name), ver, onlyActive)
  def Attribute(key: String, ver: Int, onlyActive: Boolean): Option[BaseAttributeDef] = GetReqValue(Attributes(key, onlyActive, false), ver)
  /** Answer the Active AND Current ContainerDef with the supplied namespace and name  */
  def ActiveAttribute(nameSpace: String, name: String): BaseAttributeDef = {
	  val optAttr : Option[BaseAttributeDef] = Attribute(MdMgr.MkFullName(nameSpace, name), -1, true)
	  val attr : BaseAttributeDef = optAttr match {
	    case Some(optAttr) => optAttr
	    case _ => null
	  }
	  attr
  }

  /** Get All Versions of Models */
  def Models(onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ModelDef]] = { GetImmutableSet(Some(modelDefs.flatMap(x => x._2)), onlyActive, latestVersion) }

  /** Get All Versions of Models for Key */
  def Models(key: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ModelDef]] = { GetImmutableSet(modelDefs.get(key.trim.toLowerCase), onlyActive, latestVersion) }
  def Models(nameSpace: String, name: String, onlyActive: Boolean, latestVersion: Boolean): Option[scala.collection.immutable.Set[ModelDef]] = Models(MdMgr.MkFullName(nameSpace, name), onlyActive, latestVersion)

  /** Answer the ModelDef with the supplied namespace and name  */
  def Model(nameSpace: String, name: String, ver: Int, onlyActive: Boolean): Option[ModelDef] = Model(MdMgr.MkFullName(nameSpace, name), ver, onlyActive)
  def Model(key: String, ver: Int, onlyActive: Boolean): Option[ModelDef] = GetReqValue(Models(key, onlyActive, false), ver)

  @throws(classOf[ObjectNolongerExistsException])
  def RemoveModel(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name)
      val model = modelDefs.getOrElse(key,null)
      if(model == null ){
	 logger.trace("The model " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The model $key may have been removed already")
      }
      else{
	  // Removing the model irrespective of version. Need to be fixed
	  modelDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The model " + key + " is removed ")
	      modelDefs.remove(key)
	    })

      }
  }


  @throws(classOf[ObjectNolongerExistsException])
  def DeactivateModel(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name)
      val model = modelDefs.getOrElse(key,null)
      if(model == null ){
	 logger.trace("The model " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The model $key may have been removed already")
      }
      else{
	  modelDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      m.Deactive
	    })
      }
  }


  @throws(classOf[ObjectNolongerExistsException])
  def RemoveMessage(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val message = msgDefs.getOrElse(key,null)
      if(message == null ){
	 logger.trace("The message " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The message $key may have been removed already")
      }
      else{
	  // Removing the message irrespective of version. Need to be fixed
	  msgDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The message " + key + " is removed ")
	      msgDefs.remove(key)
	    })
      }
  }


  @throws(classOf[ObjectNolongerExistsException])
  def DeactivateMessage(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val message = msgDefs.getOrElse(key,null)
      if(message == null ){
	 logger.trace("The message " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The message $key may have been removed already")
      }
      else{
	  msgDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      m.Deactive
	    })
      }
  }

  @throws(classOf[ObjectNolongerExistsException])
  def RemoveContainer(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val container = containerDefs.getOrElse(key,null)
      if(container == null ){
	 logger.trace("The container " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The container $key may have been removed already")
      }
      else{
	  // Removing the container irrespective of version. Need to be fixed
	  containerDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The container " + key + " is removed ")
	      containerDefs.remove(key)
	    })
      }
  }


  @throws(classOf[ObjectNolongerExistsException])
  def DeactivateContainer(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val container = containerDefs.getOrElse(key,null)
      if(container == null ){
	 logger.trace("The container " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The container $key may have been removed already")
      }
      else{
	  containerDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      m.Deactive
	    })
      }
  }

  @throws(classOf[ObjectNolongerExistsException])
  def RemoveFunction(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val function = funcDefs.getOrElse(key,null)
      if(function == null ){
	 logger.trace("The function " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The function $key may have been removed already")
      }
      else{
	  // Removing the function irrespective of version. Need to be fixed
	  funcDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The function " + key + " is removed ")
	      funcDefs.remove(key)
	    })
      }
  }

  @throws(classOf[ObjectNolongerExistsException])
  def RemoveAttribute(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val concept = attrbDefs.getOrElse(key,null)
      if(concept == null ){
	 logger.trace("The concept " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The concept $key may have been removed already")
      }
      else{
	  // Removing the concept irrespective of version. Need to be fixed
	  attrbDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The concept " + key + " is removed ")
	      attrbDefs.remove(key)
	    })
      }
  }

  @throws(classOf[ObjectNolongerExistsException])
  def RemoveType(nameSpace: String, name: String, ver: Int){
      val key = MdMgr.MkFullName(nameSpace, name).toLowerCase
      val typ = typeDefs.getOrElse(key,null)
      if(typ == null ){
	 logger.trace("The type " + key + " already removed ")
	 throw new ObjectNolongerExistsException(s"The type $key may have been removed already")
      }
      else{
	  // Removing the type irrespective of version. Need to be fixed
	  typeDefs(key).foreach(m =>
	    if(m.ver == ver ){
	      logger.info("The type " + key + " is removed ")
	      typeDefs.remove(key)
	    })
      }
  }

  // Make Functions. These will just make and send back the object

  /**
   *  MakeScalar catalogs any of the standard scalar types in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the scalar type namespace
   *  @param name - the scalar type name.
   */

  @throws(classOf[IllegalArgumentException])
  @throws(classOf[AlreadyExistsException])
  def MakeScalar(nameSpace: String
		      , name: String
		      , tp: Type
		      , physicalName: String
		      , ver: Int = 1
		      , jarNm: String = null
		      , depJars: Array[String] = null
			  , implementationName: String = "SomeImplementation"): ScalarTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Scalar $nameSpace.$name already exists.")
    }
    if (implementationName == null) {
      throw new IllegalArgumentException(s"Expecting ImplementationName for type.")
    }

    val st: ScalarTypeDef = new ScalarTypeDef
    SetBaseElem(st, nameSpace, name, ver, jarNm, depJars)
    st.typeArg = tp
    st.PhysicalName(physicalName)
    st.implementationName(implementationName)
    st
  }

  /**
   *  MakeTypeDef catalogs the base type def (or one of its subclasses) supplied.
   *
   *  @param nameSpace - the type namespace
   *  @param name - the type name.
   *  @param typeType - the type supplied
   */
  /*
  // physicalName may be null
  @throws(classOf[AlreadyExistsException])
  def MakeTypeDef(nameSpace: String, name: String, typeType: BaseTypeDef, physicalName: String, ver: Int, jarNm: String, depJars: Array[String]): BaseTypeDef = {
    if (Type(nameSpace, name, -1) != None) {
      throw new AlreadyExistsException(s"TypeDef $nameSpace.$name already exists.")
    }
    SetBaseElem(typeType, nameSpace, name, ver, jarNm, depJars)
    typeType.PhysicalName(physicalName)
    typeType
  }
*/

  /**
   *  MakeArray catalogs an Array based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the array type namespace
   *  @param name - the array name.
   *  @param tpNameSp - the namespace of the array element type
   *  @param tpName - the name for the element's type
   *  @param numDims - (not currently used) the number of dimensions for this array type
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeArray(nameSpace: String, name: String, tpNameSp: String, tpName: String, numDims: Int, ver: Int): ArrayTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Array $nameSpace.$name already exists.")
    }
    val elemDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The array's item type $tpNameSp.$tpName does not exist")
    if (elemDef == null) { throw new NoSuchElementException(s"The array's item type $tpNameSp.$tpName does not exist") }
    val depJarSet = scala.collection.mutable.Set[String]()
    if (elemDef.JarName != null) depJarSet += elemDef.JarName
    if (elemDef.DependencyJarNames != null) depJarSet ++= elemDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val st = new ArrayTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.elemDef = elemDef
    st.arrayDims = numDims
    st
  }

  /**
   *  MakeArrayBuffer catalogs an ArrayBuffer based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the array type name space
   *  @param name - the array name.
   *  @param tpNameSp - the name space of the array element type
   *  @param tpName - the name for the element's type
   *  @param numDims - (not currently used) the number of dimensions for this array type
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeArrayBuffer(nameSpace: String, name: String, tpNameSp: String, tpName: String, numDims: Int, ver: Int): ArrayBufTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"ArrayBuffer $nameSpace.$name already exists.")
    }
    val elemDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The array buffer's item type $tpNameSp.$tpName does not exist")
    if (elemDef == null) { throw new NoSuchElementException(s"The array buffer's item type $tpNameSp.$tpName does not exist") }
    val depJarSet = scala.collection.mutable.Set[String]()
    if (elemDef.JarName != null) depJarSet += elemDef.JarName
    if (elemDef.DependencyJarNames != null) depJarSet ++= elemDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val st = new ArrayBufTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.elemDef = elemDef
    st
  }

  /**
   *  MakeList catalogs an List based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the list's type namespace
   *  @param name - the list name.
   *  @param tpNameSp - the name space of the list item type
   *  @param tpName - the name for the element's type
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeList(nameSpace: String, name: String, tpNameSp: String, tpName: String, ver: Int): ListTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"List $nameSpace.$name already exists.")
    }
    val valDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The list's item type $tpNameSp.$tpName does not exist")
    if (valDef == null) { throw new NoSuchElementException(s"The list's item type $tpNameSp.$tpName does not exist") }
    val depJarSet = scala.collection.mutable.Set[String]()
    if (valDef.JarName != null) depJarSet += valDef.JarName
    if (valDef.DependencyJarNames != null) depJarSet ++= valDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val st = new ListTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.valDef = valDef
    st
  }

  /** 
   *  MakeQueue catalogs an Queue based type in the metadata manager's global typedefs map 
   *  
   *  @param nameSpace - the list's type namespace 
   *  @param name - the queue name.
   *  @param tpNameSp - the name space of the queue item type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   */  
   
  @throws(classOf[NoSuchElementException])
  def MakeQueue(nameSpace: String, name:String, tpNameSp: String, tpName: String, ver: Int) = {
	    if (Type(nameSpace, name, -1, false) != None) {
	      throw new AlreadyExistsException(s"List $nameSpace.$name already exists.")
	    }
	    val valDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The queue's item type $tpNameSp.$tpName does not exist")
	    if (valDef == null) { throw new NoSuchElementException(s"The queue's item type $tpNameSp.$tpName does not exist") }
	    val depJarSet = scala.collection.mutable.Set[String]()
	    if (valDef.JarName != null) depJarSet += valDef.JarName
	    if (valDef.DependencyJarNames != null) depJarSet ++= valDef.DependencyJarNames
	    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
	    val st = new QueueTypeDef
	    SetBaseElem(st, nameSpace, name, ver, null, depJars)
	    st.valDef = valDef
	    st
  }
  
  /**
   *  MakeSet catalogs an Set based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the set's type namespace
   *  @param name - the set name.
   *  @param tpNameSp - the namespace of the set's key element type
   *  @param tpName - the name for the element's type
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeSet(nameSpace: String, name: String, tpNameSp: String, tpName: String, ver: Int): SetTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Set $nameSpace.$name already exists.")
    }
    val keyDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The set's key type $tpNameSp.$tpName does not exist")
    if (keyDef == null) { throw new NoSuchElementException(s"The set's key type $tpNameSp.$tpName does not exist") }
    val depJarSet = scala.collection.mutable.Set[String]()
    if (keyDef.JarName != null) depJarSet += keyDef.JarName
    if (keyDef.DependencyJarNames != null) depJarSet ++= keyDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val st = new SetTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.keyDef = keyDef
    st
  }

  /**
   *  MakeTreeSet catalogs an TreeSet based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the set's type namespace
   *  @param name - the set name.
   *  @param tpNameSp - the namespace of the set's key element type
   *  @param tpName - the name for the element's type
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeTreeSet(nameSpace: String, name: String, tpNameSp: String, tpName: String, ver: Int): TreeSetTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"TreeSet $nameSpace.$name already exists.")
    }
    val keyDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The tree set's key type $tpNameSp.$tpName does not exist")
    if (keyDef == null) { throw new NoSuchElementException(s"The tree set's key type $tpNameSp.$tpName does not exist") }
    val depJarSet = scala.collection.mutable.Set[String]()
    if (keyDef.JarName != null) depJarSet += keyDef.JarName
    if (keyDef.DependencyJarNames != null) depJarSet ++= keyDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val st = new TreeSetTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.keyDef = keyDef
    st
  }

  	/** 
  	 *  MakeSortedSet catalogs an SortedSet based type in the metadata manager's global typedefs map 
	 *  
	 *  @param nameSpace - the set's type namespace 
	 *  @param name - the set name.
	 *  @param tpNameSp - the namespace of the set's key element type
	 *  @param tpName - the name for the element's type
	 *  @param ver - the type's version
	 */   
	// We should not have physicalName. This container type has type inside, which has PhysicalName
	@throws(classOf[AlreadyExistsException])
	@throws(classOf[NoSuchElementException])
	def MakeSortedSet(nameSpace: String, name:String, tpNameSp: String, tpName: String, ver: Int) : SortedSetTypeDef = {
		if (Type(nameSpace, name, -1, false) != None) {
		  throw new AlreadyExistsException(s"Type $nameSpace.$name already exists... unable to add SortedSet with this name.")
		}
		val keyDef = GetElem(Type(tpNameSp, tpName, -1, false), s"The tree set's key type $tpNameSp.$tpName does not exist")
		if (keyDef == null) { throw new NoSuchElementException(s"The tree set's key type $tpNameSp.$tpName does not exist") }
		val depJarSet = scala.collection.mutable.Set[String]()
		if (keyDef.JarName != null) depJarSet += keyDef.JarName
		if (keyDef.DependencyJarNames != null) depJarSet ++= keyDef.DependencyJarNames
		val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
		val st = new SortedSetTypeDef
		SetBaseElem(st, nameSpace, name, ver, null, depJars)
		st.keyDef = keyDef
		st
    }

  /**
   *  MakeMap catalogs a Map based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param key - the namespace and name for the map's key
   *  @param value - the namespace and name for the map's value
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeMap(nameSpace: String, name: String, key: (String, String), value: (String, String), ver: Int): MapTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Map $nameSpace.$name already exists.")
    }

    val (keyNmSp, keyTypeNm) = key
    val (valNmSp, valTypeNm) = value
    val keyDef = GetElem(Type(keyNmSp, keyTypeNm, -1, false), s"Key type $keyNmSp.$keyTypeNm does not exist")
    val valDef = GetElem(Type(valNmSp, valTypeNm, -1, false), s"Value type $valNmSp.$valTypeNm does not exist")
    if (keyDef == null || valDef == null) { throw new NoSuchElementException(s"Either key type ($keyNmSp.$keyTypeNm) and/or value type ($valNmSp.$valTypeNm) does not exist") }

    val depJarSet = scala.collection.mutable.Set[String]()
    if (keyDef.JarName != null) depJarSet += keyDef.JarName
    if (keyDef.DependencyJarNames != null) depJarSet ++= keyDef.DependencyJarNames
    if (valDef.JarName != null) depJarSet += valDef.JarName
    if (valDef.DependencyJarNames != null) depJarSet ++= valDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null

    val st = new MapTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.keyDef = keyDef
    st.valDef = valDef
    st
  }

  /**
   *  MakeHashMap catalogs a HashMap based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param key - the namespace and name for the map's key
   *  @param value - the namespace and name for the map's value
   *  @param ver - the version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeHashMap(nameSpace: String, name: String, key: (String, String), value: (String, String), ver: Int): HashMapTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"HashMap $nameSpace.$name already exists.")
    }

    val (keyNmSp, keyTypeNm) = key
    val (valNmSp, valTypeNm) = value
    val keyDef = GetElem(Type(keyNmSp, keyTypeNm, -1, false), s"Key type $keyNmSp.$keyTypeNm does not exist")
    val valDef = GetElem(Type(valNmSp, valTypeNm, -1, false), s"Value type $valNmSp.$valTypeNm does not exist")
    if (keyDef == null || valDef == null) { throw new NoSuchElementException(s"Either key type ($keyNmSp.$keyTypeNm) and/or value type ($valNmSp.$valTypeNm) does not exist") }

    val depJarSet = scala.collection.mutable.Set[String]()
    if (keyDef.JarName != null) depJarSet += keyDef.JarName
    if (keyDef.DependencyJarNames != null) depJarSet ++= keyDef.DependencyJarNames
    if (valDef.JarName != null) depJarSet += valDef.JarName
    if (valDef.DependencyJarNames != null) depJarSet ++= valDef.DependencyJarNames
    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null

    val st = new HashMapTypeDef
    SetBaseElem(st, nameSpace, name, ver, null, depJars)
    st.keyDef = keyDef
    st.valDef = valDef
    st
  }

  /**
   *  MakeTupleType catalogs a Tuple based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param tuples - an array of (namespace,typename) pairs that correspond to each tuple element
   *  @param ver - the version info
   *
   *  Note: Between one and twenty-two elements may be specified in the tuples array.
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeTupleType(nameSpace: String, name: String, tuples: Array[(String, String)], ver: Int): TupleTypeDef = {
    if (Type(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Typle $nameSpace.$name already exists.")
    }

    val depJarSet = scala.collection.mutable.Set[String]()

    var tupleDefs: ArrayBuffer[BaseTypeDef] = new ArrayBuffer[BaseTypeDef]()
    tuples.foreach(tup => {
      val (nmspc, nm): (String, String) = tup
      val tupType: BaseTypeDef = GetElem(Type(nmspc, nm, -1, false), s"Tuple element type $nmspc.$nm does not exist")
      if (tupType == null) { throw new NoSuchElementException(s"Tuple element type $nmspc.$nm does not exist") }
      if (tupType.JarName != null) depJarSet += tupType.JarName
      if (tupType.DependencyJarNames != null) depJarSet ++= tupType.DependencyJarNames
      tupleDefs += tupType
    })

    val depJars = if (depJarSet.size > 0) depJarSet.toArray else null
    val tt: TupleTypeDef = new TupleTypeDef
    tt.tupleDefs = tupleDefs.toArray
    SetBaseElem(tt, nameSpace, name, ver, null, depJars)
    tt
  }

 /**
   *  Construct a FunctionDef from the supplied arguments.
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the function name that all are to be cataloged in the nameSpace argument.
   *  @param physicalName - this is the "full" package qualified function name
   *  @param retTypeNsName - a tuple (return type namespace and name)
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details


   *  @param ver - the version of the function
   *  @param jarNm - where this function is housed
   *  @param depJars - the list of jars that this function depends upon 
   *   
   *  @return a FunctionDef
   *
   * 	FIXME: The version, the jarNm, and dependency jars... are these really appropriate for object creation?
   *  		Can these be supplied at catalog time (i.e., the Add... function)?  Do we really want the list of all
   *    	these jars on every function cataloged in the metadata manager?  
   *     
   *     	I believe with prudent construction of the classpath, the only jar that need to be supplied is the 
   *      	jar containing the function.  Furthermore, it is really the full object path (e.g., 
   *        com.ligadata.pmml.udfs.Udfs that has many, many, many functions) where this jar association is needed.
   *        Any component then would get the package path of the function to look up the jar for 100s or functions
   *        at one crack.  Detailing this repetitive information on all FunctionDef instances seems TOO MUCH to me.
   *        
   *        Similarly, the version information associated with the function should be expressed at the 
   *        package level.  Functions as I see it are released a package at a time.  All the functions defined
   *        in that package should have the same version information.
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeFunc(nameSpace: String
		      , name: String
		      , physicalName: String
		      , retTypeNsName: (String, String)
		      , args: List[(String, String, String)]
		      , fmfeatures : Set[FcnMacroAttr.Feature]
			  , ver: Int = 1
			  , jarNm: String = null
			  , depJars: Array[String] = null): FunctionDef = {
    
    if (Function(nameSpace, name, args.map(a => (a._2, a._3)), -1, false) != None) {
      throw new AlreadyExistsException(s"Function $nameSpace.$name already exists.")
    }
    val fn: FunctionDef = new FunctionDef
    if (fmfeatures != null && fmfeatures.size > 0) {
    	fn.features ++= fmfeatures
    }
    val (retTypeNs, retType) = retTypeNsName
    fn.retType = GetElem(Type(retTypeNs, retType, -1, false), s"Return type $retTypeNs.$retType does not exist")
    if (fn.retType == null) { throw new NoSuchElementException(s"Return type $retTypeNs.$retType does not exist") }

    val depJarSet = scala.collection.mutable.Set[String]()
    if (fn.retType.JarName != null) depJarSet += fn.retType.JarName
    if (fn.retType.DependencyJarNames != null) depJarSet ++= fn.retType.DependencyJarNames

    fn.args = args.map(elem => {
      val arg = MakeArgDef(elem._1, elem._2, elem._3)
      if (arg.DependencyJarNames != null) depJarSet ++= arg.DependencyJarNames
      arg
    }).toArray

    if (depJars != null) depJarSet ++= depJars
    val dJars = if (depJarSet.size > 0) depJarSet.toArray else null

    SetBaseElem(fn, nameSpace, name, ver, jarNm, dJars)
    fn.PhysicalName(physicalName)
    fn.active = true
    fn
  }

  /**
   *  Construct a list of FunctionDef from the supplied arguments.  All function names share the same namespace, return type and arguments.
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param names - a list of function names that all are to be cataloged in the nameSpace argument.
   *  @param retTypeNsName - a tuple (return type namespace and name) shared by all of the function names
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details

   *  @param ver - the version of the function
   *  @param jarNm - where this function is housed
   *  @param depJars - the list of jars that this function depends upon 
   *   
   *  @return a List[FunctionDef]
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeFuncs(nameSpace: String
		  		, names: List[(String, String)]
  				, retTypeNsName: (String, String)
  				, args: List[(String, String, String)]
  				, fmfeatures : Set[FcnMacroAttr.Feature]
  				, ver: Int
  				, jarNm: String
  				, depJars: Array[String]): List[FunctionDef] = {
    val funcs = names.map(name => {
      val (logicalNm, physicalName) = name
      MakeFunc(nameSpace, logicalNm, physicalName, retTypeNsName, args, fmfeatures, ver, jarNm, depJars)
    }).toList
    funcs
  }

  
  
  /**
   * MakeMacro adds a MacroDef to two maps in the metadata manager supplied.  A MacroDef is very similar 
   * in many respect to a function definition and is looked up in the same way as a function by the 
   * pmml compiler. In fact it is a subclass of FunctionDef.  The macros are used by the pmml compiler 
   * to manage several kinds of code generation issues.
   * 
   * NOTE: There is a huge effort underway in the Scala community to provide a general macro capability
   * that will vastly expand the power of the Scala programming language, offering powerful meta programming
   * capabilities.  Unfortunately, these language extensions are a work in progress with things being added
   * (and jerked out) from release to release of the language.  At some point, I believe we will be able
   * to adapt their macro generation approach for our purposes.  Until that time arrives, the macros 
   * we are supporting here are quite limited (by comparison) but also will nevertheless be useful for 
   * extending the pmml code generation process.
   * 
   *  @param nameSpace - the namespace of this macro
   *  @param name - the macro name 
   *  @param retTypeNsName - a tuple (return type namespace and name) 
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param macrofeatures - a set of hints that describe the sort of macro being cataloged.  See mdelems.scala for details
   *  @param macroTemplateStr - a tuple that has templates for both the fixed message case and the mapped message case.  When
   *  		no containers are in use in the macro, the same template will be used.  See the other MakeMacro function for
   *    	clarification of what was said here.
   *  @return a MacroDef
 
   */
  @throws(classOf[NoSuchElementException])
  def MakeMacro(nameSpace: String
			  	, name:String
			  	, retTypeNsName: (String,String)
			  	, args: List[(String, String, String)]
			  	, macrofeatures : Set[FcnMacroAttr.Feature]
		  		, macroTemplateStr : (String, String)
		  		, ver: Int = 1) : MacroDef = {

    val aMacro : MacroDef = new MacroDef
    SetBaseElem(aMacro, nameSpace, name, ver, null, null)

    if (macrofeatures != null && macrofeatures.size > 0) {
    	aMacro.features ++= macrofeatures
    }
    aMacro.macroTemplate = macroTemplateStr
    
    val (retTypeNs, retType) = retTypeNsName
    aMacro.retType = GetElem(Type(retTypeNs, retType, -1, false), s"Return type $retTypeNs.$retType does not exist")
    if (aMacro.retType == null) { throw new NoSuchElementException(s"Return type $retTypeNs.$retType does not exist") }
    
    aMacro.args = args.map(elem => {
    	MakeArgDef(elem._1, elem._2, elem._3)
    }).toArray
    
    aMacro
  }
  
  /** 
   *  This version of MakeMacro accepts just one template string.  It is useful for those cases where the macro
   *  does not contain a container that might be fixed or mapped.  The same template is used regardless.
   *  @param nameSpace - the namespace of this macro
   *  @param name - the macro name 
   *  @param retTypeNsName - a tuple (return type namespace and name) 
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param macrofeatures - a set of hints that describe the sort of macro being cataloged.  See mdelems.scala for details
   *  @param macroTemplateStr - a tuple that has templates for both the fixed message case and the mapped message case.  When
   *  		no containers are in use in the macro, the same template will be used.  See the other MakeMacro function for
   *    	clarification of what was said here.
   *  @return a MacroDef
   */
  @throws(classOf[NoSuchElementException])
  def MakeMacro(nameSpace: String
			  	, name:String
			  	, retTypeNsName: (String,String)
			  	, args: List[(String, String, String)]
			  	, macrofeatures : Set[FcnMacroAttr.Feature]
		  		, macroTemplateStr : String
		  		, ver: Int) : MacroDef = {
	  MakeMacro(nameSpace, name, retTypeNsName, args, macrofeatures, (macroTemplateStr, macroTemplateStr), ver)
  }

   /**
   *  Construct and catalog a "fixed structure" message container with the supplied named attributes. They may be of arbitrary
   *  types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param className - the fully qualified name of the class that will represent the runtime instance of the message
   *  @param args - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeFixedMsg(nameSpace: String
			      , name: String
			      , physicalName: String
			      , args: List[(String, String, String, String, Boolean)]
				  , ver: Int = 1
				  , jarNm: String = null
				  , depJars: Array[String] = null): MessageDef = {

	if (Message(nameSpace, name, -1, false) != None) {
	  throw new AlreadyExistsException(s"Message $nameSpace.$name already exists.")
	}
	var msg: MessageDef = new MessageDef
	msg.containerType = MakeStructDef(nameSpace, name, physicalName, args, ver, jarNm, depJars)
	
	var dJars: Array[String] = depJars
	if (msg.containerType.isInstanceOf[ContainerTypeDef]) // This should match
	  dJars = msg.containerType.asInstanceOf[ContainerTypeDef].DependencyJarNames // Taking all dependencies for Container type. That has everything is enough for this
	
	SetBaseElem(msg, nameSpace, name, ver, jarNm, dJars)
	msg.PhysicalName(physicalName)
	msg
  }

  
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeFixedContainer(nameSpace: String
				      , name: String
				      , physicalName: String
				      , args: List[(String, String, String, String, Boolean)]
					  , ver: Int = 1
					  , jarNm: String = null
					  , depJars: Array[String] = null): ContainerDef = {

    if (Container(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Container $nameSpace.$name already exists.")
    }
    var container = new ContainerDef
    container.containerType = MakeStructDef(nameSpace, name, physicalName, args, ver, jarNm, depJars)

    var dJars: Array[String] = depJars
    if (container.containerType.isInstanceOf[ContainerTypeDef]) {// This should match
	    // Taking all dependencies for Container type. That has everything is enough for this
	    dJars = container.containerType.asInstanceOf[ContainerTypeDef].DependencyJarNames 
    }
    SetBaseElem(container, nameSpace, name, ver, jarNm, dJars)

    container.PhysicalName(physicalName)
    container
  }

  
  /**
   *  Construct and catalog a "map based" message container with an arbitrary number of named attributes with <b>HETEROGENEOUS</b> types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param argTypes - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   *  @return the constructed message as a modicum of convenience
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeMappedMsg(nameSpace: String
			      , name: String
			      , physicalName: String
			      , args: List[(String, String, String, String, Boolean)]
				  , ver: Int
				  , jarNm: String
				  , depJars: Array[String]): MessageDef = {

    if (Message(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Message $nameSpace.$name already exists.")
    }

    var msg: MessageDef = new MessageDef
    msg.containerType = MakeContainerTypeMap(nameSpace, name, physicalName, args, ver, jarNm, depJars)

    var dJars: Array[String] = depJars
    if (msg.containerType.isInstanceOf[ContainerTypeDef]) // This should match
      dJars = msg.containerType.asInstanceOf[ContainerTypeDef].DependencyJarNames // Taking all dependencies for Container type. That has everything is enough for this

    SetBaseElem(msg, nameSpace, name, ver, jarNm, dJars)
    msg.PhysicalName(physicalName)
    msg
  }

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeMappedContainer(nameSpace: String
					      , name: String
					      , physicalName: String
					      , args: List[(String, String, String, String, Boolean)]
						  , ver: Int
						  , jarNm: String
						  , depJars: Array[String]): ContainerDef = {
    if (Container(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Container$nameSpace.$name already exists.")
    }
    var container = new ContainerDef
    container.containerType = MakeContainerTypeMap(nameSpace, name, physicalName, args, ver, jarNm, depJars)

    var dJars: Array[String] = depJars
    if (container.containerType.isInstanceOf[ContainerTypeDef]) // This should match
      dJars = container.containerType.asInstanceOf[ContainerTypeDef].DependencyJarNames // Taking all dependencies for Container type. That has everything is enough for this

    SetBaseElem(container, nameSpace, name, ver, jarNm, dJars)

    container.PhysicalName(physicalName)
    container
  }

  /**
   *  Construct and catalog a "map based" message container with an arbitrary number of <b>HOMOGENEOUSLY</b> typed value attributes.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param argTypes - a List of triples (attribute name, attribute type namespace, attribute type name)
   *  @return the constructed message as a modicum of convenience
   */
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def MakeMappedMsg(nameSpace: String
			      , name: String
			      , physicalName: String
			      , argTypNmSpName: (String, String)
			      , argNames: List[String]
				  , ver: Int
				  , jarNm: String
				  , depJars: Array[String]): MessageDef = {
    if (Message(nameSpace, name, -1, false) != None) {
      throw new AlreadyExistsException(s"Message $nameSpace.$name already exists.")
    }

    val msgNm = MdMgr.MkFullName(nameSpace, name)

    val (typeNmSp, typeName) = argTypNmSpName
    val args = argNames.map(elem => (msgNm, elem, typeNmSp, typeName, false)) //BUGBUG::Making all local Attributes

    var msg: MessageDef = new MessageDef
    msg.containerType = MakeContainerTypeMap(nameSpace, name, physicalName, args, ver, jarNm, depJars)

    var dJars: Array[String] = depJars
    if (msg.containerType.isInstanceOf[ContainerTypeDef]) // This should match
      dJars = msg.containerType.asInstanceOf[ContainerTypeDef].DependencyJarNames // Taking all dependencies for Container type. That has everything is enough for this

    SetBaseElem(msg, nameSpace, name, ver, jarNm, dJars)
    msg.PhysicalName(physicalName)

    msg
  }

  /**
   *  Construct and catalog a model definition.  The model definition represents the essential metadata regarding
   *  a PMML generated model, including the identifying information, the model type, and the inputs and output
   *  variables used/generated by the model.  The input and output information is used by the online learning engine
   *  manager to generate the necessary order execution between the scheduled model working set.
   *
   *  @param nameSpace - the namespace in which this model should be cataloged
   *  @param name - the name of the model.
   *  @param className - the fully qualified className for the compiled model.
   *  @param inputVars - a List of quintuples (model var namespace (could be msg or concept or modelname), variable name, type namespace
   *  					, type name, and whether the variable is global (a concept that has been independently cataloged))
   *  @param outputVars - a List of pairs (output variable name, output variable type).  Note that the output variable inherits
   *  			its model's namespace.
   *  @return the ModelDef instance as a measure of convenience
   *
   */

  def MakeModelDef(nameSpace: String
			      , name: String
			      , physicalName: String
			      , modelType: String
			      , inputVars: List[(String, String, String, String, Boolean)]
				  , outputVars: List[(String, String, String)]
				  , ver: Int = 1
				  , jarNm: String = null
				  , depJars: Array[String] = null): ModelDef = {

    val modelToBeReplaced: Boolean = (Model(nameSpace, name, -1, false) != None)
    if (modelToBeReplaced) {
      throw new AlreadyExistsException(s"Model $nameSpace.$name already exists.")
    }
    val mdl: ModelDef = new ModelDef
    mdl.PhysicalName(physicalName)
    mdl.modelType = modelType
    val depJarSet = scala.collection.mutable.Set[String]()

    val mdlNm = MdMgr.MkFullName(nameSpace, name)
    // val mdlOutputVars = outputVars.map(o => (mdlNm, o._1, o._2, o._3))

    mdl.outputVars = outputVars.map(o => {
      val (nm, typnsp, typenm) = o
      val atr = MakeAttribDef(mdlNm, nm, typnsp, typenm, ver, false)
      if (atr.JarName != null) depJarSet += atr.JarName
      if (atr.DependencyJarNames != null) depJarSet ++= atr.DependencyJarNames
      atr
    }).toArray

    mdl.inputVars = inputVars.map(elem => {
      val (varNameSp, varName, typeNameNs, typeName, isGlobal) = elem
      val atr = MakeAttribDef(varNameSp, varName, typeNameNs, typeName, ver, isGlobal)
      if (atr.JarName != null) depJarSet += atr.JarName
      if (atr.DependencyJarNames != null) depJarSet ++= atr.DependencyJarNames
      atr
    }).toArray

    if (depJars != null) depJarSet ++= depJars
    val dJars = if (depJarSet.size > 0) depJarSet.toArray else null

    SetBaseElem(mdl, nameSpace, name, ver, jarNm, dJars)

    mdl
  }

  // Add Functions
  //
  /**
   *  MakeScalar catalogs any of the standard scalar types in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the scalar type namespace
   *  @param name - the scalar type name.
   */

  @throws(classOf[AlreadyExistsException])
  def AddScalar(nameSpace: String
		      , name: String
		      , tp: Type
		      , physicalName: String
		      , ver: Int = 1
		      , jarNm: String = null
		      , depJars: Array[String] = null
    		  , implementationName: String = null): Unit = {
    AddScalar(MakeScalar(nameSpace, name, tp, physicalName, ver, jarNm, depJars, implementationName))
  }

  @throws(classOf[AlreadyExistsException])
  def AddScalar(st: ScalarTypeDef): Unit = {
    if (Type(st.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"Scalar ${st.FullName} already exists.")
    }
    typeDefs.addBinding(st.FullName, st)
  }

  /**
   *  AddArray catalogs an Array based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the array type namespace
   *  @param name - the array name.
   *  @param tpNameSp - the namespace of the array element type
   *  @param tpName - the name for the element's type
   *  @param numDims - (not currently used) the number of dimensions for this array type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddArray(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , numDims: Int
		      , ver: Int): Unit = {
    AddArray(MakeArray(nameSpace, name, tpNameSp, tpName, numDims, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddArray(at: ArrayTypeDef): Unit = {
    if (Type(at.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"Array ${at.FullName} already exists.")
    }
    typeDefs.addBinding(at.FullName, at)
  }

  /**
   *  AddArrayBuffer catalogs an ArrayBuffer based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the array type name space
   *  @param name - the array name.
   *  @param tpNameSp - the name space of the array element type
   *  @param tpName - the name for the element's type
   *  @param numDims - (not currently used) the number of dimensions for this array type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddArrayBuffer(nameSpace: String
			      , name: String
			      , tpNameSp: String
			      , tpName: String
			      , numDims: Int
			      , ver: Int): Unit = {
    AddArrayBuffer(MakeArrayBuffer(nameSpace, name, tpNameSp, tpName, numDims, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddArrayBuffer(abt: ArrayBufTypeDef): Unit = {
    if (Type(abt.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"ArrayBuffer ${abt.FullName} already exists.")
    }
    typeDefs.addBinding(abt.FullName, abt)
  }

  /**
   *  AddList catalogs an List based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the list's type namespace
   *  @param name - the list name.
   *  @param tpNameSp - the name space of the list item type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddList(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , ver: Int): Unit = {
    AddList(MakeList(nameSpace, name, tpNameSp, tpName, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddList(lst: ListTypeDef): Unit = {
    if (Type(lst.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"List ${lst.FullName} already exists.")
    }
    typeDefs.addBinding(lst.FullName, lst)
  }

  
  /**
   *  AddQueue catalogs an Queue based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the list's type namespace
   *  @param name - the list name.
   *  @param tpNameSp - the name space of the list item type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddQueue(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , ver: Int): Unit = {
    AddQueue(MakeQueue(nameSpace, name, tpNameSp, tpName, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddQueue(queue: QueueTypeDef): Unit = {
    if (Type(queue.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"A type with queue's name ${queue.FullName} already exists.")
    }
    typeDefs.addBinding(queue.FullName, queue)
  }

  /**
   *  AddSet catalogs an Set based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the set's type namespace
   *  @param name - the set name.
   *  @param tpNameSp - the namespace of the set's key element type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddSet(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , ver: Int): Unit = {
    AddSet(MakeSet(nameSpace, name, tpNameSp, tpName, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddSet(set: SetTypeDef): Unit = {
    if (Type(set.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"Set ${set.FullName} already exists.")
    }
    typeDefs.addBinding(set.FullName, set)
  }

  /**
   *  MakeTreeSet catalogs an TreeSet based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the set's type namespace
   *  @param name - the set name.
   *  @param tpNameSp - the namespace of the set's key element type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddTreeSet(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , ver: Int): Unit = {
    AddTreeSet(MakeTreeSet(nameSpace, name, tpNameSp, tpName, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddTreeSet(tree: TreeSetTypeDef): Unit = {
    if (Type(tree.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"TreeSet ${tree.FullName} already exists.")
    }
    typeDefs.addBinding(tree.FullName, tree)
  }

  /**
   *  AddSortedSet catalogs an SortedSetTypeDef based upon the supplied parameters.
   *
   *  @param nameSpace - the set's type namespace
   *  @param name - the set name.
   *  @param tpNameSp - the namespace of the set's key element type
   *  @param tpName - the name for the element's type
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddSortedSet(nameSpace: String
		      , name: String
		      , tpNameSp: String
		      , tpName: String
		      , ver: Int): Unit = {
    AddSortedSet(MakeSortedSet(nameSpace, name, tpNameSp, tpName, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddSortedSet(set: SortedSetTypeDef): Unit = {
    if (Type(set.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"SortedSet ${set.FullName} cannot be created... a type by that name already exists.")
    }
    typeDefs.addBinding(set.FullName, set)
  }


  /**
   *  MakeMap catalogs a Map based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param key - the namespace and name for the map's key
   *  @param value - the namespace and name for the map's value
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddMap(nameSpace: String
		      , name: String
		      , key: (String, String)
		      , value: (String, String)
		      , ver: Int): Unit = {
    AddMap(MakeMap(nameSpace, name, key, value, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddMap(map: MapTypeDef): Unit = {
    if (Type(map.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"Map ${map.FullName} already exists.")
    }
    typeDefs.addBinding(map.FullName, map)
  }

  /**
   *  MakeHashMap catalogs a HashMap based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param key - the namespace and name for the map's key
   *  @param value - the namespace and name for the map's value
   *  @param ver - version info
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddHashMap(nameSpace: String
		      , name: String
		      , key: (String, String)
		      , value: (String, String)
		      , ver: Int): Unit = {
    AddHashMap(MakeHashMap(nameSpace, name, key, value, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddHashMap(hmap: HashMapTypeDef): Unit = {
    if (Type(hmap.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"HashMap ${hmap.FullName} already exists.")
    }
    typeDefs.addBinding(hmap.FullName, hmap)
  }

  /**
   *  MakeTupleType catalogs a Tuple based type in the metadata manager's global typedefs map
   *
   *  @param nameSpace - the map type's namespace
   *  @param name - the map name.
   *  @param tuples - an array of (namespace,typename) pairs that correspond to each tuple element
   *  @param ver - version info
   *
   *  Note: Between one and twenty-two elements may be specified in the tuples array.
   *
   */
  // We should not have physicalName. This container type has type inside, which has PhysicalName
  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddTupleType(nameSpace: String
			      , name: String
			      , tuples: Array[(String, String)]
	    		  , ver: Int): Unit = {
    AddTupleType(MakeTupleType(nameSpace, name, tuples, ver))
  }

  @throws(classOf[AlreadyExistsException])
  def AddTupleType(tt: TupleTypeDef): Unit = {
    if (Type(tt.FullName, -1, false) != None) {
      throw new AlreadyExistsException(s"Typle ${tt.FullName} already exists.")
    }
    typeDefs.addBinding(tt.FullName, tt)
  }

  /**
   *  Construct a FunctionDef from the supplied arguments and add it to this MdMgr instance
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the function name that all are to be cataloged in the nameSpace argument.
   *  @param physicalName - FIXME: find out what this is and document it .... is this full class name?????????????????? 
   *  @param retTypeNsName - a tuple (return type namespace and name)
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details.

   *  @param ver - the version of the function
   *  @param jarNm - where this function is housed
   *  @param depJars - the list of jars that this function depends upon 
   *
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddFunc(nameSpace: String
		      , name: String
		      , physicalName: String
		      , retTypeNsName: (String, String)
		      , args: List[(String, String, String)]
			  , fmfeatures : Set[FcnMacroAttr.Feature]
			  , ver: Int = 1
			  , jarNm: String = null
			  , depJars: Array[String] = Array[String]()): Unit = {
      AddFunc(MakeFunc(nameSpace, name, physicalName, retTypeNsName, args, fmfeatures, ver, jarNm, depJars))
  }

  /**
   *  Catalog the supplied FunctionDef in this MdMgr instance.  Should it already exist, the catalog 
   *  operation is rejected with an AlreadyExistsException.
   *
   *  @param fn the FunctionDef
   *
   */

  @throws(classOf[AlreadyExistsException])
  def AddFunc(fn: FunctionDef): Unit = {
    val args = fn.args.map(a => a.aType.FullName).toList
    if (Function(fn.FullName, args, -1, false) != None) {
    	val argsStr = args.mkString(",")
		throw new AlreadyExistsException(s"Function ${fn.FullName} with arguments \'${argsStr}\' already exists.")
    }
    val fcnSig : String = fn.typeString
    if (FunctionByTypeSig(fcnSig) != null) {
    	throw new AlreadyExistsException(s"Function ${fn.FullName} with signature \'${fcnSig}\' already exists.")
    }
	if (MacroByTypeSig(fcnSig) != null) {
		throw new AlreadyExistsException(s"Macro ${fn.FullName} with signature \'${fcnSig}\' will be hidden should this function be cataloged.")
	}

    funcDefs.addBinding(fn.FullName, fn)
    compilerFuncDefs(fcnSig.toLowerCase()) = fn /** add it to the pmml compiler's typesignature based map as well */ 

  }


  /**
   *  Catalog the supplied AttributeDef in this MdMgr instance.  Should it already exist, the catalog 
   *  operation is rejected with an AlreadyExistsException.
   *
   *  @param attr the AttributeDef
   *
   */

  @throws(classOf[AlreadyExistsException])
  def AddAttribute(attr: BaseAttributeDef): Unit = {
    attrbDefs.addBinding(attr.FullName, attr)
  }


  @throws(classOf[AlreadyExistsException])
  def MakeConcept(nameSpace: String
		   , name: String
		   , typeNameNs: String
		   , typeName: String
		   , ver: Int = 1
		   , isGlobal: Boolean): BaseAttributeDef = {
    val attr = MakeAttribDef(nameSpace, name, typeNameNs, typeName, ver, isGlobal)
    attr
  }


  /**
   *  Construct a list of FunctionDef from the supplied arguments.  All function names share the same namespace, return type and arguments.
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param names - a list of function names that all are to be cataloged in the nameSpace argument.
   *  @param retTypeNsName - a tuple (return type namespace and name) shared by all of the function names
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details

   *  @param ver - the version of the function
   *  @param jarNm - where this function is housed
   *  @param depJars - the list of jars that this function depends upon 
   *  @return a List[FunctionDef]
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddFuncs(nameSpace: String
		      , names: List[(String, String)]
			  , retTypeNsName: (String, String)
			  , args: List[(String, String, String)]
			  , fmfeatures : Set[FcnMacroAttr.Feature]
			  , ver: Int
			  , jarNm: String
			  , depJars: Array[String]): Unit = {
	  AddFuncs(MakeFuncs(nameSpace, names, retTypeNsName, args, fmfeatures, ver, jarNm, depJars))
  }

  /**
   *  Construct a list of FunctionDef from the supplied arguments.
   *
   *  @param fns - a List[FunctionDef] to catalog in this MdMgr instance
   */

  @throws(classOf[AlreadyExistsException])
  def AddFuncs(fns: List[FunctionDef]): Unit = {
      fns.foreach(fn => {
    	  val args = fn.args.map(a => a.aType.FullName).toList
		  if (Function(fn.FullName, args, -1, false) != None) {
			  val argsStr = args.mkString(",")
			  throw new AlreadyExistsException(s"Function ${fn.FullName} with arguments \'${argsStr}\' already exists.")
		  }
		  val fcnSig : String = fn.typeString
		  if (FunctionByTypeSig(fcnSig) != null) {
		      throw new AlreadyExistsException(s"Function ${fn.FullName} with signature \'${fcnSig}\' already exists.")
		  }
		  if (MacroByTypeSig(fcnSig) != null) {
		      throw new AlreadyExistsException(s"Macro ${fn.FullName} with signature \'${fcnSig}\' will be hidden should this function be cataloged.")
		  }

      })
      fns.foreach(fn => {
    	  funcDefs.addBinding(fn.FullName, fn)
		  val key = fn.typeString
		  compilerFuncDefs(key.toLowerCase()) = fn /** add it to the pmml compiler's typesignature based map as well */ 
      })
  }

  /**
   *  Construct a MacroDef from the supplied arguments and add it to this MdMgr instance
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the function name that all are to be cataloged in the nameSpace argument.
   *  @param retTypeNsName - a tuple (return type namespace and name)
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details
   *  @param macroTemplateStrs - a Tuple2.  when the function has CLASSUPDATE feature, the first macro in the pair 
   *  		has the "builds" template has a template that supports "fixed" container field update.  The second string
   *    	in the tuple contains the "mapped" container template for same function.  When the CLASSUPDATE feature is
   *     	not specified, the same macro template can be specified for both tuple._1 and ._2.  A convenience 
   *      	method is available that handles this for you.


   *  @param ver - the version of the function
   *
   */

  def AddMacro(nameSpace: String
			  	, name:String
			  	, retTypeNsName: (String,String)
			  	, args: List[(String, String, String)]
			  	, macrofeatures : Set[FcnMacroAttr.Feature]
		  		, macroTemplateStrs : (String, String)
		  		, ver: Int = 1) : Unit = {
      AddMacro(MakeMacro(nameSpace, name, retTypeNsName, args, macrofeatures, macroTemplateStrs, ver))
  }

  /**
   *  Construct a MacroDef from the supplied arguments and add it to this MdMgr instance
   *
   *  @param nameSpace - the namespace in which this structure type
   *  @param name - the function name that all are to be cataloged in the nameSpace argument.
   *  @param retTypeNsName - a tuple (return type namespace and name)
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param args - a List of triples (argument name, argument type namespace and type name), one for each argument
   *  @param fmfeatures - a set of hints that describe the sort of function being cataloged.  See mdelems.scala for details
   *  @param macroTemplateStrs - a Tuple2.  when the function does not support CLASSUPDATE feature, supply just
   *  	one template to this method. 


   *  @param ver - the version of the function
   *
   */

  def AddMacro(nameSpace: String
			  	, name:String
			  	, retTypeNsName: (String,String)
			  	, args: List[(String, String, String)]
			  	, macrofeatures : Set[FcnMacroAttr.Feature]
		  		, macroTemplateStr : String
		  		, ver: Int) : Unit = {
      AddMacro(MakeMacro(nameSpace, name, retTypeNsName, args, macrofeatures, macroTemplateStr, ver))
  }

  /**
   *  Catalog the supplied MacroDef to this MdMgr instance.  Should one exist with the identical 
   *  type string, the catalog operation is rejected with an AlreadyExistsException.
   *
   *  @param mac - the MacroDef to be cataloged FunctionByTypeSig(key : String): FunctionDef = { compilerFuncDefs.get(key).getOrElse(null) }
   *  MacroByTypeSig(key : String)
   */

  @throws(classOf[AlreadyExistsException])
  def AddMacro(mac: MacroDef): Unit = {
    val macroSignature : String = mac.typeString
    if (MacroByTypeSig(macroSignature) != null) {
      throw new AlreadyExistsException(s"Macro ${mac.FullName} with signature \'${macroSignature}\' already exists.")
    }
    if (FunctionByTypeSig(macroSignature) != null) {
      throw new AlreadyExistsException(s"Warning! Macro ${mac.FullName} with signature \'${macroSignature}\' is hidden by a function with the same signature.")
    }

    macroDefSets.addBinding(mac.FullName, mac)
    macroDefs(macroSignature.toLowerCase()) = mac 
  }
 
  
  /**
   *  Construct and catalog a "fixed structure" message with the supplied named attributes. They may be of arbitrary
   *  types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param className - the fully qualified name of the class that will represent the runtime instance of the message
   *  @param args - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddFixedMsg(nameSpace: String
			      , name: String
			      , physicalName: String
			      , args: List[(String, String, String, String, Boolean)]
				  , ver: Int = 1
				  , jarNm: String = null
				  , depJars: Array[String] = Array[String]()): Unit = {
	  AddMsg(MakeFixedMsg(nameSpace, name, physicalName, args, ver, jarNm, depJars))
  }

  /**
   *  Construct and catalog a "map based" message with an arbitrary number of named attributes with  <b>HETEROGENEOUS</b> types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param argTypes - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   *  @return the constructed message as a modicum of convenience
   */

  @throws(classOf[AlreadyExistsException])
  @throws(classOf[NoSuchElementException])
  def AddMappedMsg(nameSpace: String
			      , name: String
			      , physicalName: String
			      , args: List[(String, String, String, String, Boolean)]
				  , ver: Int = 1
				  , jarNm: String = null
				  , depJars: Array[String] = Array[String]()): Unit = {
	  AddMsg(MakeMappedMsg(nameSpace, name, physicalName, args, ver, jarNm, depJars))
  }

  /**
   *  Construct and catalog a "map based" message container with an arbitrary number of <b>HOMOGENEOUSLY</b> typed value attributes.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param argTypes - a List of triples (attribute name, attribute type namespace, attribute type name)
   *  @return the constructed message as a modicum of convenience
   */
  	@throws(classOf[AlreadyExistsException])
  	@throws(classOf[NoSuchElementException])
  	def AddMappedMsg(nameSpace: String
		  			, name: String
		  			, physicalName: String
		  			, argTypNmSpName: (String, String)
		  			, argNames: List[String]
				 	, ver: Int 
				 	, jarNm: String
		  			, depJars: Array[String]): Unit = {
	  	AddMsg(MakeMappedMsg(nameSpace, name, physicalName, argTypNmSpName, argNames, ver, jarNm, depJars))
  	}

  	@throws(classOf[AlreadyExistsException])
	@throws(classOf[NoSuchElementException])
	def AddMsg(msg: MessageDef): Unit = {
		if (Type(msg.FullName, -1, false) != None) {
		  throw new AlreadyExistsException(s"Message type ${msg.FullName} already exists.")
		}
		if (Message(msg.FullName, -1, false) != None) {
		  throw new AlreadyExistsException(s"Message ${msg.FullName} already exists.")
		}
		if( msg.containerType == null ){
		  throw new NoSuchElementException(s"The containerType of the Message ${msg.FullName} can not be null.")
		}
		val typ = msg.containerType.asInstanceOf[ContainerTypeDef]
		typeDefs.addBinding(typ.FullName, typ)
		msgDefs.addBinding(msg.FullName, msg)
  	}

  /**
   *  Construct and catalog a "fixed structure" container with the supplied named attributes. They may be of arbitrary
   *  types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param className - the fully qualified name of the class that will represent the runtime instance of the message
   *  @param args - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   */

  	@throws(classOf[AlreadyExistsException])
  	@throws(classOf[NoSuchElementException])
  	def AddFixedContainer(nameSpace: String
				  	    , name: String
				  	    , physicalName: String
				  	    , args: List[(String, String, String, String, Boolean)]
					  	, ver: Int = 1
					  	, jarNm: String = null
					  	, depJars: Array[String] = Array[String]()): Unit = {
	  	AddContainer(MakeFixedContainer(nameSpace, name, physicalName, args, ver, jarNm, depJars))
  	}

  /**
   *  Construct and catalog a "map based" container with an arbitrary number of named attributes with HETEROGENEOUS types.
   *
   *  @param nameSpace - the namespace in which this message should be cataloged
   *  @param name - the name of the message.
   *  @param argTypes - a List of attributes information (attribute namespace, attribute name, attribute type namespace, attribute type name, isGlobal)
   *  @return the constructed message as a modicum of convenience
   */

	@throws(classOf[AlreadyExistsException])
	@throws(classOf[NoSuchElementException])
	def AddMappedContainer(nameSpace: String
						, name: String
						, physicalName: String
						, args: List[(String, String, String, String, Boolean)]
					  	, ver: Int = 1
					  	, jarNm: String = null
						, depJars: Array[String] = Array[String]()): Unit = {
	    AddContainer(MakeMappedContainer(nameSpace, name, physicalName, args, ver, jarNm, depJars))
	}

	@throws(classOf[AlreadyExistsException])
	@throws(classOf[NoSuchElementException])
	def AddContainer(container: ContainerDef): Unit = {
	    if (Type(container.FullName, -1, false) != None) {
	      throw new AlreadyExistsException(s"Container type ${container.FullName} already exists.")
	    }
	    if (Container(container.FullName, -1, false) != None) {
	      throw new AlreadyExistsException(s"Container ${container.FullName} already exists.")
	    }
	    if( container.containerType == null ){
	      throw new NoSuchElementException(s"The containerType of container ${container.FullName} can not be null.")
	    }
	    val typ = container.containerType.asInstanceOf[ContainerTypeDef]
	    typeDefs.addBinding(typ.FullName, typ)
	    containerDefs.addBinding(container.FullName, container)
	}

  	/**
  	 *  Construct and catalog a model definition.  The model definition represents the essential metadata regarding
  	 *  a PMML generated model, including the identifying information, the model type, and the inputs and output
  	 *  variables used/generated by the model.  The input and output information is used by the online learning engine
  	 *  manager to generate the necessary order execution between the scheduled model working set.
  	 *
  	 *  @param nameSpace - the namespace in which this model should be cataloged
  	 *  @param name - the name of the model.
  	 *  @param className - the fully qualified className for the compiled model.
  	 *  @param inputVars - a List of quintuples (model var namespace (could be msg or concept or modelname), variable name, type namespace, and type name)
  	 *  @param outputVars - a List of pairs (output variable name, output variable type).  Note that the output variable inherits
  	 *  			its model's namespace.
  	 *  @return the ModelDef instance as a measure of convenience
  	 *
  	 */
  	def AddModelDef(nameSpace: String
  					, name: String
  					, physicalName: String
  					, modelType: String
  					, inputVars: List[(String, String, String, String, Boolean)]
					, outputVars: List[(String, String, String)]
					, ver: Int = 1
					, jarNm: String = null
					, depJars: Array[String] = Array[String]()): Unit = {
	  	AddModelDef(MakeModelDef(nameSpace, name, physicalName, modelType, inputVars, outputVars, ver, jarNm, depJars))
  	}

  	def AddModelDef(mdl: ModelDef): Unit = {
	  	if (Model(mdl.FullName, -1, false) != None) {
	  		throw new AlreadyExistsException(s"Model ${mdl.FullName} already exists.")
	  	}
	  	modelDefs.addBinding(mdl.FullName, mdl)
  	}
  	// External Functions -- End 

}

object MdIdSeq {
  var increment: Long = 1
  var minValue: Long = 1
  var maxValue: Long = 2 ^ 63 - 1
  var cacheSize: Long = 100
  var start: Long = 1

  var current: Long = start
  var consumeLimit: Long = start

  def next: Long = {
    val nxt = current
    current += 1
    nxt
  }
}

trait LogTrait {
  val loggerName = this.getClass.getName
  lazy val logger = Logger.getLogger(loggerName)
}

object MdMgr extends LogTrait {

  /** Static variables */
  val mdMgr = new MdMgr;
  val sysNS = "System" // system name space
  var IdCntr = 0; // metadata elem unique id counter; later use proper class and make it persistent

  /**
   *  Answer the default metadata manager advertised in this object.
   *
   *  @return mdMgr
   */
  def GetMdMgr = mdMgr

  def SysNS = sysNS

  /** Helper function to form a proper search key */
  def MkFullName(nameSpace: String, name: String): String = (nameSpace.trim + "." + name.trim).toLowerCase // Ignoring version for now

  /** Helper function to form a proper search key */
  def MkFullNameWithVersion(nameSpace: String, name: String): String = MkFullNameWithVersion(nameSpace, name, -1)
  /** Helper function to form a proper search key */
  def MkFullNameWithVersion(nameSpace: String, name: String, ver: Int): String = (nameSpace.trim + "." + name.trim + "." + ver).toLowerCase // Ignoring version for now

}



