package com.ligadata.Compiler

import scala.collection.mutable._
import scala.collection.immutable.{ Set }
import org.xml.sax.Attributes
import com.ligadata.olep.metadata._
import com.ligadata.Pmml.Runtime._
import com.ligadata.OnLEPBase._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._
import scala.util.control.Breaks._

/** 
3) PmmlContext is a global container for the application.  It has a number of key containers and variables:
		
	a) pmmlNodeQueue contains those elements pulled from the Pmml file traverse that are to be processed further.
	b) pmmlExecNodeQueue contains decorated versions of the PmmlNodeQueue.
	c) pmmlNodeStack contains PmmlNodes that are of interest during the XML tree navigation.  The start events
	allocate new PmmlNode instances of the appropriate subtype corresponding to the XML detected and push them to 
	this stack.
	d) To specialize handling of the various element types and their child elements, a number of dispatch tables
	are used:  val fs = Map("John" -> helloJohn _, "Joe" -> helloJoe _)
		i) pmmlElementVistitorMap(qName -> visitorFcn(ctx:PmmlContext, currNode:PmmlNode[+A]) - this will collect 
		the appropriate information from the element/child element being currently visited.  Should a child element
		not be represented in this visitor map, it and its contents are ignored and the traversal continues.
		ii) pmmlXformDispatchMap(pmmlNodeName -> xFormFcn(ctx:PmmlContext, currNode:PmmlNode[+A]) - this is a
		dispatch map that knows how to transform the raw PmmlNode derivative to a corresponding PmmlExecNode
		derivative.
		iii) pmmlFcnNameDerivationMap (pmmlNodeName -> derivationFcn(ctx:PmmlContext, currNode:PmmlNode[+A]) - this
		function will create a unique name for the function represented by this PmmlNode.
		iv) pmmlBuiltinMap (pmmlBuiltinFcnName -> builtinFcn(ctx:PmmlContext, currNode:PmmlExecNode[+A])
		v) pmmlCodeGeneratorMap (pmmlNodeName -> codeGeneratorFcn(ctx:PmmlContext, currNode:PmmlExecNode[+A]) - this map
		will generate appropriate Scala source to represent this node for emission.
	e) a sequence number generator that will be used to help disambiguate names used for functions.
	f) To manage the variables in the Pmml file's DataDictionary and TransformationDictionary the type information about
	the variables contained there are kept in the dataFieldMap(fieldName -> PmmlDataTypeInfo) and 
	derivedFieldMap(derivedFieldName -> PmmlDataTypeInfo) respectively.
	g) To manage the variables to be returned by the model, the MiningField elements marked with usageType value
	'predicted' or 'supplementary' are gathered in a list, modelOutput : List[Any].  These are emitted at the end of
	the object main function to return the model scoring results to the scoring component's client.
	h) log4j logger is available.
	

*/

class PmmlContext(val mgr : MdMgr, val injectLogging : Boolean)  extends LogTrait {	
	/** used to generate a unique number for fcn names in same scope during scala code generation */
	var counter : Int = 0
	def Counter() : Int = { 
	  counter += 1 
	  counter 
	}
	
	var clientName : String = "unknown"
	def ClientName(nm : String) { clientName = nm }
	def ClientName : String = clientName
	
	/** pmmlNodeQueue stores the root elements of interest out of the xml parse for further parsing by xForm */ 
	var pmmlNodeQueue = new Queue[PmmlNode]()
	/** pmmlNodeStack makes the parent node available during the xml parse so node tree parent/child relationships can be made */
	var pmmlNodeStack  = new Stack[PmmlNode]()

	/** 
	 	pmmlExecNodesStack makes the parent node available during the PmmlNode -> PmmlExecNode transformation .. a number of children
	 	only update the parent rather than establish a full child node 
	 */
	var pmmlExecNodeStack = new Stack[Option[PmmlExecNode]]()
	/** 
	 *  This map will contain the Execution node trees by name after the parse and transformation.  They are 
	 *  used by name in order to generate the source code snips in the correct order 
	 */
	var pmmlExecNodeMap = new HashMap[String, Option[PmmlExecNode]]()
	
	/** 
	 *  This queue enqueues update classes so that apply methods can explicitly change state of a Container field via function.
	 *  Classes are needed so that the container or field to be updated can be expressed as a 'var'.  Given the heavily nested 
	 *  uses of functions in a pmml derived field, it is difficult to update a field and yet still participate in the predicate.
	 *  By doing update and returning true if the update was successful, these direct update functions can participate in the
	 *  regular flow of the predicates at work in a given derived field's apply function.
	 */
	var updateClassQueue = new Queue[String]()
	def UpdateClassQueue : Queue[String] = updateClassQueue
	
	/** Make refs to the DataDictionary and TransformationDictionary in those respective containers for convenience */
	var dDict : HashMap[String,xDataField] = HashMap[String,xDataField]()
	def DataDict : HashMap[String,xDataField] = dDict
	def DataDict( d : HashMap[String,xDataField]) { dDict = d }

	/** 
	 * Answer the full package qualified names of the Scala Objects that contain udfs. These are to be added to the
	 * import list of the generated Scala for the model. It is up to the modeler to specify which UDF objects to use
	 * in the model.
	 *
	 * By design, these full package qualified object names must be specified as enumerated values in the data dictionary
	 * element named "UDFSearchPath".
	 *
	 */
	def udfSearchPath : Array[String] = {
		val pathDataField : xDataField = if (dDict.contains("UDFSearchPath")) dDict("UDFSearchPath") else null
		val srchPath : Array[String] = if (pathDataField != null) {
	    	pathDataField.values.map( valPair => valPair._1).toArray
	  	} else {
	    	Array[String]()
	  	}
	  	srchPath
	}

	var xDict : HashMap[String,xDerivedField] = HashMap[String,xDerivedField]()
	def TransformDict : HashMap[String,xDerivedField] = xDict
	def TransformDict( d : HashMap[String,xDerivedField]) { xDict = d }
	
	/** Some of the metadata lookup and navigation methods are found in the helper */
	val mdHelper : MetadataInterpreter = new MetadataInterpreter(this)
	def MetadataHelper : MetadataInterpreter = mdHelper
	
	/** This value used in all map/filter/groupBy apply functions as the element reference */
	val applyElementName : String = "_each"
	  
	/** While processing elements this stack tracks where we are in the transformation dictionary generation */
	val elementStack : Stack[PmmlExecNode] = Stack[PmmlExecNode]()
	/** FcnTypeInfo stack ... used to decide whether field expressions should be fixed ( e.g., ctr.field) or mapped (e.g., ctr("field")) 
	 *  especially used for iterable functions. */
	val fcnTypeInfoStack : Stack[FcnTypeInfo] = Stack[FcnTypeInfo]()
	
	/** FIXME: This needs to be pulled from either the metadata manager or possibly specified
	 *  in some way in the PMML model itself.  At the moment this is hard coded to get something
	 *  working. 
	 */
	val namespaceSearchPath : Array[String] = Array[String](MdMgr.sysNS, "Pmml")
	
	
	def NameSpaceSearchPath : String = {
		val buffer : StringBuilder = new StringBuilder
		buffer.append("{")
		namespaceSearchPath.addString(buffer, ",")
		buffer.append("}")
		buffer.toString
	}
	
	/** 
	 *  When expandCompoundFieldTypes is specified, any container.subcontainer.field... reference has the type 
	 *  information for each  portion of the container returned in the array.  When false, only the leaf type (the field name's type)
	 *  is returned.
	 *  
	 *  @param fldName - a name, possibly with a namespace and/or a container '.' prefix and field name key
	 *  @param expandCompoundFieldTypes - when true the container type (if present) and the field type are 
	 *  	returned.  When false, only the field is returned.
	 *  @return an Array of (typestring, isContainerWithNamedFields, BaseTypeDef) triples
	 */
	def getFieldType(fldname : String, expandCompoundFieldTypes : Boolean) : Array[(String,Boolean,BaseTypeDef)] = {	
		val names : Array[String] = if (fldname.contains(".")) {
			val fldNames : Array[String] = fldname.split('.')
			val isDictItem : Boolean = (dDict.contains(fldNames(0)) || xDict.contains(fldNames(0)))
			/** fold keys to lower case except for dictionary names when hiearchical container field reference detected */
			if (isDictItem) {
				var arr : ArrayBuffer[String] = ArrayBuffer[String]()
				arr += fldNames(0)
				fldNames.tail.foreach(itm => arr += itm.toLowerCase)
				arr.toArray
			} else {
			  	fldname.split('.').map(_.toLowerCase) 
			 }
		} else {
			Array(fldname)
		}
		mdHelper.getFieldType(names, expandCompoundFieldTypes)
	}
	
	/** 
	 *  FIXME:  arbitrarily compound fields now supported.  This function SCHEDULED FOR REMOVAL - NOT USED.
	 *  
	 *  FIXME : Temporarily no container nesting allowed ... just to get something going.
	 *  A complete implementation will take into consideration those notes made in the 
	 *  PmmlIdentifierEvaluationNodes.txt found in src/main/resources.
	 *  
	 *  Answer the  field type of some field 'nm' in 'container'.  If not found "None" is returned.
	 *  When expandCompoundFieldTypes is true, the container type and the field type are both returned, assuming the 
	 *  field in its container are legitimate names.
	 *  
	 */
	
	def getFieldType(nmSpc : String, container : String, nm : String, expandCompoundFieldTypes : Boolean) : Array[(String,Boolean,BaseTypeDef)] = {	
		val scalaType : Array[(String,Boolean,BaseTypeDef)] = if (dDict.contains(container)) {
			/** for data dictionary and transaction dictionary search, ignore the namespace ... use it
			 *  for the mdmgr search. */
			val ctnrFld : xDataField = dDict.apply(container)
			/** get the container type def for this field from metadata */
			val containerTypedef : ContainerTypeDef = getContainerType(nmSpc, ctnrFld.dataType)
			val containerTypeString : String = containerTypedef.typeString
			if (containerTypedef != null) {
			  /** FIXME: only structured types for now... add maps, sets, arrays, et al in a bit*/
			  var attrDef : BaseAttributeDef = null
			  val lowerCaseName = nm.toLowerCase()
			  val memberType = containerTypedef match {
				  case str : StructTypeDef => {
				     //	memberDefs: Array[BaseAttributeDef] = _
					  val memberDefs : Array[BaseAttributeDef] = containerTypedef.asInstanceOf[StructTypeDef].memberDefs
					  val mDef : Array[BaseAttributeDef] = memberDefs.filter( m => m.name == lowerCaseName )
					  if (mDef.size > 0) {
						  attrDef = mDef.head
						  attrDef.typeString 
					  } else {
					    "Any"
					  }
				  }
				  case _ => containerTypeString
				}
			  	if (expandCompoundFieldTypes && attrDef != null) {
			  		/** FIXME: attrDef could be container when hierarchical structs supported. */
			  		Array[(String,Boolean,BaseTypeDef)]((containerTypeString,true, containerTypedef), (memberType,false, attrDef.typeDef))
			  	} else {
			  		Array[(String,Boolean,BaseTypeDef)]((memberType, false, containerTypedef))
			  	}
			} else {
				PmmlError.logError(this, s"getFieldType($nmSpc, $container, $nm) dDict didnt produce a container for this container search")
				Array[(String,Boolean,BaseTypeDef)](("None",false,null))		
			}
		} else {
			val inXDict : Boolean = xDict.contains(container)
			val scalaTypeFromXDict : Array[(String,Boolean,BaseTypeDef)] = if (inXDict) {
				val ctnrFld : xDerivedField = xDict.apply(container)
				/** get the container type def for this field from metadata */
				val containerTypedef : ContainerTypeDef = getContainerType(nmSpc, ctnrFld.dataType)
				val containerTypeString : String = containerTypedef.typeString
				if (containerTypedef != null) {
				  /** FIXME: only structured types for now... add maps, sets, arrays, et al in a bit*/
				  var attrDef : BaseAttributeDef = null
				  val lowerCaseName = nm.toLowerCase()
				  val memberType = containerTypedef match {
					  case str : StructTypeDef => {
						  val memberDefs : Array[BaseAttributeDef] = containerTypedef.asInstanceOf[StructTypeDef].memberDefs
						  val mDef : Array[BaseAttributeDef] = memberDefs.filter( m => m.name == lowerCaseName )
						  if (mDef.size > 0) {
							  attrDef = mDef.head
							  attrDef.typeString 
						  } else {
						    "Any"
						  }
					  }
					  case _ => containerTypeString
					}
				  	if (expandCompoundFieldTypes && attrDef != null) {
				  		/** FIXME: attrDef could be container when hierarchical structs supported. */
				  		Array[(String,Boolean,BaseTypeDef)]((containerTypeString,true,containerTypedef), (memberType,false,attrDef.typeDef))
				  	} else {
				  		val containerTypeStr : String = containerTypedef.typeString
				  		logger.debug(s"... container found memberType = $memberType, containerTypedef = $containerTypeStr")
				  		Array[(String,Boolean,BaseTypeDef)]((memberType, true, containerTypedef))
				  	}
				} else {
					val cntrName : String = ctnrFld.name
					PmmlError.logError(this, s"getFieldType($nmSpc, $container, $nm) no type found in mdmgr for $cntrName found in xDict")
					Array[(String,Boolean,BaseTypeDef)](("None",false,null))
				}
			} else {
				PmmlError.logError(this, s"getFieldType($nmSpc, $container, $nm) xDict didn't produce a container for this container search")
				Array[(String,Boolean,BaseTypeDef)](("None",false,null))
			} 
			scalaTypeFromXDict
		}
		scalaType
	}

	/** 
	 *  FIXME:  arbitrarily compound fields now supported.  This function SCHEDULED FOR REMOVAL - NOT USED.
	 *  
	 *  The two argument field type search assumes that this namespace/name search on attribute/field.
	 *  The dDict and xDict are searched with and without the namespace as a key.
	 *  If in neither, the standard attribute search is done with both the nmspace and name in the mgr
	 */
	def getFieldType(nmSpc : String, nm : String) : (String, Boolean, BaseTypeDef) = {	
	
		var scalaType : String = null
		var fldTypedef : BaseTypeDef = null
		val withNameSpace : Boolean = true
		val withOutNameSpace : Boolean = false
		var isContainer : Boolean = false

		val (s,f) : (String,BaseTypeDef) = getDDictFieldType(nmSpc, nm, withOutNameSpace)
		if (s != null) {
			scalaType = s
			fldTypedef = f
		} else {
			val (s,f) : (String,BaseTypeDef) = getDDictFieldType(nmSpc, nm, withNameSpace)
			if (s != null) {
				scalaType = s
				fldTypedef = f
			} else {
				val (s,f) = getXDictFieldType(nmSpc, nm, withOutNameSpace)
				if (s != null) {
				  	scalaType = s
				  	fldTypedef = f
				} else {
					val (s,f) : (String,BaseTypeDef) = getXDictFieldType(nmSpc, nm, withNameSpace)
					if (s != null) {
						scalaType = s
						fldTypedef = f
					} else {
						val key : String = MdMgr.MkFullName(nmSpc, nm)
						val elem : BaseTypeDef = mgr.ActiveType(key)
						val (s,f) : (String,BaseTypeDef) = if (elem != null) (elem.typeString,elem) else ("None",null)
						if (s != null) {
							scalaType = s
							fldTypedef = f
						}
					}
				}
			}
		}
		isContainer = (fldTypedef != null && fldTypedef.isInstanceOf[ContainerTypeDef])

		(scalaType,isContainer,fldTypedef)
	}
		
	/** 
	 *  FIXME:  arbitrarily compound fields now supported.  This function SCHEDULED FOR REMOVAL - NOT USED.
	 * 
	 *  Helper ... search the dDict with supplied key 
	 */
	private def getDDictFieldType(nmSpc : String, nm : String, withNmSpc : Boolean) : (String,BaseTypeDef) = {	
		val key : String = if (withNmSpc) nmSpc + "." + nm else nm
		var fldTypedef : BaseTypeDef = null
		val scalaType : String = if (dDict.contains(key)) {
			val fld : xDataField = dDict.apply(key)
			val fldType = fld.dataType
			fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, fldType)).asInstanceOf[BaseTypeDef]
			if (fldTypedef != null) {
				fldTypedef.typeString
			} else {
				PmmlError.logError(this, s"getXDictFieldType: unknown type $fldType")
				null  // allow processing to continue.. even though this is likely a disaster for the compile
			}
		} else null 
		(scalaType,fldTypedef)
	}

	/** 
	 *  FIXME:  arbitrarily compound fields now supported.  This function SCHEDULED FOR REMOVAL - NOT USED.
	 * 
	 *  Helper ... search the xDict with supplied key 
	 *   
	 */
	private def getXDictFieldType(nmSpc : String, nm : String, withNmSpc : Boolean) : (String,BaseTypeDef) = {	
		val key : String = if (withNmSpc) nmSpc + "." + nm else nm
		var fldTypedef : BaseTypeDef = null
		val scalaType : String = if (xDict.contains(key)) {
			val fld : xDerivedField = xDict.apply(key)
			val fldType = fld.dataType
			fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, fldType)).asInstanceOf[BaseTypeDef]
			if (fldTypedef != null) {
				fldTypedef.typeString
			} else {
				/** Translate the possible builtin type to a scala equiv and try again. */
				val scalaVersionOfPmmlBuiltinType : String = PmmlTypes.scalaDataType(fldType)
				/** verify it is present in the metadata and get metadata description for it */
				fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, scalaVersionOfPmmlBuiltinType))
				if (fldTypedef == null) {
					PmmlError.logError(this, s"getXDictFieldType: unknown type $scalaVersionOfPmmlBuiltinType ($fldType)")
					null  // allow processing to continue.. even though this is likely a disaster for the compile
				} else {
					fldTypedef.typeString
				}
			}
		} else null 
		(scalaType,fldTypedef)
	}

	/** 
	 *  Get the container's typedef associated with the supplied namespace and name. 
	 */
	def getContainerType(nmSpc : String, container : String) : ContainerTypeDef = {
		val baseTypeDef : BaseTypeDef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, container))
		val containerTypeDef = baseTypeDef match {
		  case c : ContainerTypeDef => baseTypeDef.asInstanceOf[ContainerTypeDef]
		  case _ => {
		    PmmlError.logError(this, s"getContainerType: the container $nmSpc.$container is not a container or message.  Did you forget to catalog it?")
		    val objStr : String = if (baseTypeDef == null) "null" else baseTypeDef.toString
		    PmmlError.logError(this, s"getContainerType: the object returned is $objStr")
		    null
		  }
		}

	  	containerTypeDef
	}
	

	/** 
	 *  Cache the MiningSchemaMap from the xRuleSetModel node here for more convenient access during code generation
	 */
	var miningSchemaMap : HashMap[String,xMiningField] = HashMap[String,xMiningField]()
	def MiningSchemaMap(m : HashMap[String,xMiningField]) { miningSchemaMap = m }
	def MiningSchemaMap : HashMap[String,xMiningField] = miningSchemaMap
	
	/** 
	 *  This map contains values picked up during parse and later used to generate the Scala source code.  The following KEYS
	 *  MUST be present at code generation time to properly identify the generated model source.:
	 *  	ApplicationName (from the Header)
	 *   	FunctionName (from the RuleSetModel ... and other model types we are to support)
	 *   
	 *  The name of the actual PMML source file is also collected
	 *  	PMML (see the PmmlCompiler.scala source for use)
	 *      
	 *  These will be used if present:
	 *   	Version (string version from the Header)
	 *    	ApplicationName (from the Header)
	 *    	Copyright (from the Header)
	 *      Description (from the Header)
	 *      ModelName (from the RulesetModel ... and other model types we are to support)
	 *      ClassName (the derived class name based upon the ApplicationName and Version)
	 *      VersionNumber  (a numeric version number... only dec digits extracted
	 */
	var pmmlTerms : HashMap[String,Option[String]] = HashMap[String,Option[String]]() 
	
	/** this is from command line parameters... those Ole events that this model can/should handle */
	var eventsHandled : ArrayBuffer[String] = ArrayBuffer[String]()
	
	/** 
	 *  [(message name, (appears in ctor signature?, message type for named message, varName))] 
	 *  
	 *  The containersInScope is used to create message and container references in generaal.  Those that are
	 *  messages (declared in the "messages" data dictionary field), will also be generated as part of the 
	 *  generated model's main constructor
	 */
	var containersInScope : ArrayBuffer[(String, Boolean, BaseTypeDef, String)] = ArrayBuffer[(String, Boolean, BaseTypeDef, String)]()
	
	/** Capture field ref variables that are inputs for the model under consideration for model def generation.  For now, this 
	 *  means container field references.  Note: Other kinds of inputs are anticipated.  What makes it into this Map is a moving target.
	 *  
	 *  ctx.modelInputs(fldRef.field.toLowerCase()) = (cName, fieldName, baseType.NameSpace, baseType.Name, isGlobal)
	 *  is the statement used to add one to the map.  The cName is the container name, the field name the container's field, the
	 *  baseType namespace and name are the field's base type's namespace and name, and the boolean is an indication if this is a 
	 *  global (was cataloged in the attrdef map explicitly). 
	 */
	var modelInputs : Map[String, (String, String, String, String, Boolean, String)] = Map[String, (String, String, String, String, Boolean, String)]()
	
	/** Model outputs are collected here for ModelDef generation. 
	 *  The triple consists of the field name, field's type namespace, and the field type */
	var modelOutputs : Map[String, (String, String, String)] = Map[String, (String, String, String)]()
	
	/** 
	 *  Register any messages that will appear in the constructor of the generated model class.  Register the
	 *  the gCtx so it can be added to the constructor even though it is used only in "Get" functions in the 
	 *  use of it in the derived fields.  Allow ContainerDefs as well as MessageDefs there as well as there is 
	 *  little to distinguish them from each other save their names.
	 *  
	 *  NOTE: Containers are encountered during semantic analysis as types are examined on each DataField
	 *  and DerivedField.  These are added to the containersInScope map as well.  
	 */ 
	def RegisterMessages : Unit = {
		val messages : xDataField = if (dDict.contains("parameters")) dDict.apply("parameters") else null
		if (messages == null) {
			logger.error("No input message(s) specified for this model. Please specify messages variable with one or more message names as values.")
		} else {
			messages.values.foreach( value => {			
				val msgFldName : String =  value._1 
				val msgFld : xDataField = if (dDict.contains(msgFldName)) dDict.apply(msgFldName) else null
				val tuple = if (msgFld != null) {
					val msgDef : MessageDef = mgr.ActiveMessage(MdMgr.SysNS, msgFld.dataType)
					if (msgDef == null) {
						val containerDef : BaseTypeDef = mgr.ActiveType(MdMgr.SysNS, msgFld.dataType)
						if (containerDef == null) {
							logger.error("The supplied message has no corresponding message definition.  Please add metadata for this message.")
						} else {
							if (containerDef.isInstanceOf[ContainerTypeDef]) {
								containersInScope += Tuple4(msgFldName,true,containerDef,msgFldName)
							} else {
								logger.error(s"MessageDef encountered that did not have a container type def... type = ${containerDef.typeString}")	
							}
							/** This is a convenient place to pick up the jars needed to compile and execute the model under construction */
							val implJar : String  = containerDef.JarName
							val depJars : Array[String] = containerDef.DependencyJarNames
							collectClassPathJars(implJar, depJars)
						}
					} else {
						val containerDef : BaseTypeDef = mgr.ActiveType(MdMgr.SysNS, msgFld.dataType)
						if (containerDef == null) {
							logger.error("The supplied message has no corresponding message definition.  Please add metadata for this message.")
						} else {
							/** This is a convenient place to pick up the jars needed to compile and execute the model under construction */
							val implJar : String  = containerDef.JarName
							val depJars : Array[String] = containerDef.DependencyJarNames
							collectClassPathJars(implJar, depJars)
						  
							if (containerDef.isInstanceOf[ContainerTypeDef]) {
								containersInScope += Tuple4(msgFldName,true,containerDef,msgFldName)
							} else {
								logger.error(s"MessageDef encountered that did not have a container type def... type = ${containerDef.typeString}")	
							}
						}
					}
				} else {
					logger.error("The input message referenced in the messages field has not been declared in the data dictionary.  Do that before proceeding.")
				}
			})
		}
	}
	
	/** Mechanism to collect the container, message and (soon... function) jars for the class path */
	val classPathJars : scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
	
	/** 
	 *  Used by RegisterMessages and RegisterContainerAsNecessarys, et al... add the implementation jar and dependency jars 
	 *  for each msg,container, and fcn element  
	 */
	def collectClassPathJars(implJar : String, depJars : Array[String]) : Unit = {
		if (implJar != null) {
			classPathJars.add(implJar)
		}
		if (depJars != null && depJars.size > 0) {
			depJars.foreach(jar => classPathJars.add(jar))
		}
	}
	
	/** Check all of the xDerivedFields for dataTypes that are either MessageDefs or ContainerDefs 
	 *  Record them in the context's containersInScope with any containers collected in the DataDictionary's
	 *  "parameters" field.
	 */
	def collectContainers {
		xDict.foreach( fld => {
			val (name, dFld) : (String, xDerivedField) = fld
			RegisterContainerAsNecessary(name, dFld.dataType)
		})
	}
	

	/** 
	 *  Use this function to register any containers and/or messages that are being used by this model.
	 *  This function is called during semantic analysis of DataField and DerivedField elements.  Since they
	 *  didn't appear in the parameter list, instances of them are to be obtained from the gCtx with a Get 
	 *  method call.
	 *  
	 *  NOTE: variable name is not required for the gCtx related containers.  It is used just for the messages
	 *  mentioned in the parameters data field used to prepare the main model constructor.
	 */
	def RegisterContainerAsNecessary(name : String, dataType : String) : Boolean = {
		val elem : BaseTypeDef = mgr.ActiveType(MdMgr.SysNS, dataType)
		val registered : Boolean = if (elem != null) {
			/** This is a convenient place to pick up the jars needed to compile and execute the model under construction */
			val implJar : String  = elem.JarName
			val depJars : Array[String] = elem.DependencyJarNames
			collectClassPathJars(implJar, depJars)
			
			elem match {
			  case con : ContainerTypeDef => { 
				  			containersInScope += Tuple4(name,false,elem.asInstanceOf[ContainerTypeDef], "n/a")
				  			true
				  		}
			  case _ => { 	//logger.debug(s"Unecessary to register this dataType ... $dataType")
				  			false
			    		}
			} 
		} else { 
		  false
		}
		
		registered
	}
	
	/** these get queued for further processing */
	val topLevelContainers : List[String] = List[String]("Header", "DataDictionary", "TransformationDictionary", "RuleSetModel")

	/** 
	 *  This object substitutes pattern map templates with arguments found in the parameter lists of the apply functions in the pmml 
	 *  trees, principally the TransactionDictionary.
	 */
	val fcnSubstitute : FcnSubstitution =  new FcnSubstitution
	
	/** Udf map ... used for generating type appropriate constants, etc */
	var udfMap : HashMap[String, PmmlExecNode] = HashMap[String, PmmlExecNode]()
	def UdfMap : HashMap[String, PmmlExecNode] = udfMap

	/** 
	 *  RuleSetModel and SimpleRule subclass constructor statements collected here for instantiation in the model class' initialize method 
	 *  The pair is ("RuleSetModel" -> Set("new RuleSetModel(....)") or ("SimpleRule" -> Set("new SimpleRule_RULE1_01(....)", "new SimpleRule_RULE2_07(...)", ...) 
	 */
	val simpleRuleInsertionOrder : ArrayBuffer[String] =  ArrayBuffer[String]()
	val ruleRuleSetInstantiators = new HashMap[String, scala.collection.mutable.Set[String]] with MultiMap[String, String]
	def RuleRuleSetInstantiators : HashMap[String, scala.collection.mutable.Set[String]] with MultiMap[String, String] = ruleRuleSetInstantiators
	var miningSchemaInstantiators : HashMap[String,String] = HashMap[String, String]()
	
	/** RuleSetModel attributes collected by a PmmlExecNodeVisitor */
	var defaultScore : String = "None"
	def DefaultScore : String = { defaultScore }
	def DefaultScore(defltScore : String) { defaultScore = defltScore }
	var ruleSetSelectionMethods : ArrayBuffer[xRuleSelectionMethod] = new ArrayBuffer[xRuleSelectionMethod]()
	def RuleSetSelectionMethods(arrayOfMethods : ArrayBuffer[xRuleSelectionMethod]) { ruleSetSelectionMethods = arrayOfMethods }
	def RuleSetSelectionMethods : ArrayBuffer[xRuleSelectionMethod] = ruleSetSelectionMethods 
	
	/** SimpleRule attributes collected by a PmmlExecNodeVisitor.  These are added in the order they are parsed.. They are
	 *  used to update the respective SimpleRule instances added to the RuleSetModel's SimpleRule array */
	var scoreDistributions : ArrayBuffer[ArrayBuffer[xScoreDistribution]] = ArrayBuffer[ArrayBuffer[xScoreDistribution]]()
	def RuleScoreDistributions(arrayOfDistros : ArrayBuffer[xScoreDistribution]) { scoreDistributions += arrayOfDistros }
	def RuleScoreDistributions(idx : Int) : ArrayBuffer[xScoreDistribution] = { scoreDistributions.apply(idx) }
	def RuleScoreDistributions : ArrayBuffer[ArrayBuffer[xScoreDistribution]] = { scoreDistributions }
	
	/** Node name => function map for data collection */
    type ElementVisitFcn = (String,String,String,Attributes) => PmmlNode
    val  pmmlElementVistitorMap = Map[String, ElementVisitFcn]()
    pmmlElementVistitorMap += ("Constant" -> PmmlNode.mkPmmlConstant)
    pmmlElementVistitorMap += ("Header" -> PmmlNode.mkPmmlHeader)
    pmmlElementVistitorMap += ("Application" -> PmmlNode.mkPmmlApplication)
    pmmlElementVistitorMap += ("DataDictionary" -> PmmlNode.mkPmmlDataDictionary)
    pmmlElementVistitorMap += ("DataField" -> PmmlNode.mkPmmlDataField)
    pmmlElementVistitorMap += ("Interval" -> PmmlNode.mkPmmlInterval)
    pmmlElementVistitorMap += ("Value" -> PmmlNode.mkPmmlValue)
    pmmlElementVistitorMap += ("TransformationDictionary" -> PmmlNode.mkPmmlTransformationDictionary)
    pmmlElementVistitorMap += ("DerivedField" -> PmmlNode.mkPmmlDerivedField)
    pmmlElementVistitorMap += ("Apply" -> PmmlNode.mkPmmlApply)
    pmmlElementVistitorMap += ("FieldRef" -> PmmlNode.mkPmmlFieldRef)
    pmmlElementVistitorMap += ("MapValues" -> PmmlNode.mkPmmlMapValues)
    pmmlElementVistitorMap += ("FieldColumnPair" -> PmmlNode.mkPmmlFieldColumnPair)
    pmmlElementVistitorMap += ("InlineTable" -> PmmlNode.mkPmmlInlineTable)
    pmmlElementVistitorMap += ("row" -> PmmlNode.mkPmmlrow)
    pmmlElementVistitorMap += ("RuleSetModel" -> PmmlNode.mkPmmlRuleSetModel)
    pmmlElementVistitorMap += ("SimpleRule" -> PmmlNode.mkPmmlSimpleRule)
    pmmlElementVistitorMap += ("ScoreDistribution" -> PmmlNode.mkPmmlScoreDistribution)
    pmmlElementVistitorMap += ("CompoundPredicate" -> PmmlNode.mkPmmlCompoundPredicate)
    pmmlElementVistitorMap += ("SimpleSetPredicate" -> PmmlNode.mkPmmlSimpleSetPredicate)
    pmmlElementVistitorMap += ("SimplePredicate" -> PmmlNode.mkPmmlSimplePredicate)
    pmmlElementVistitorMap += ("MiningSchema" -> PmmlNode.mkPmmlMiningSchema)
    pmmlElementVistitorMap += ("MiningField" -> PmmlNode.mkPmmlMiningField)
    pmmlElementVistitorMap += ("RuleSet" -> PmmlNode.mkPmmlRuleSet)
    pmmlElementVistitorMap += ("RuleSelectionMethod" -> PmmlNode.mkPmmlRuleSelectionMethod)
    pmmlElementVistitorMap += ("DefineFunction" -> PmmlNode.mkPmmlDefineFunction)
    pmmlElementVistitorMap += ("ParameterField" -> PmmlNode.mkPmmlParameterField)
    pmmlElementVistitorMap += ("Array" -> PmmlNode.mkPmmlArray)


   
    
	/** Dispatcher for PMML xml data collection */
	def dispatchElementVisitor(ctx : PmmlContext, namespaceURI: String, localName: String , qName:String, atts: Attributes) {
		var node : PmmlNode = qName match {
			case "Constant" => PmmlNode.mkPmmlConstant(namespaceURI, localName, qName, atts)
			case "Header" => PmmlNode.mkPmmlHeader(namespaceURI, localName, qName, atts)
			case "Application" => PmmlNode.mkPmmlApplication(namespaceURI, localName, qName, atts)
			case "DataDictionary" => PmmlNode.mkPmmlDataDictionary(namespaceURI, localName, qName, atts)
			case "DataField" => PmmlNode.mkPmmlDataField(namespaceURI, localName, qName, atts)
			case "Value" => PmmlNode.mkPmmlValue(namespaceURI, localName, qName, atts)
			case "Interval" => PmmlNode.mkPmmlInterval(namespaceURI, localName, qName, atts)
			case "TransformationDictionary" => PmmlNode.mkPmmlTransformationDictionary(namespaceURI, localName, qName, atts)
			case "DerivedField" => PmmlNode.mkPmmlDerivedField(namespaceURI, localName, qName, atts)
			case "DefineFunction" => PmmlNode.mkPmmlDefineFunction(namespaceURI, localName, qName, atts)
			case "ParameterField" => PmmlNode.mkPmmlParameterField(namespaceURI, localName, qName, atts)
			case "Apply" => PmmlNode.mkPmmlApply(namespaceURI, localName, qName, atts)
			case "FieldRef" => PmmlNode.mkPmmlFieldRef(namespaceURI, localName, qName, atts)
			case "MapValues" => PmmlNode.mkPmmlMapValues(namespaceURI, localName, qName, atts)
			case "FieldColumnPair" => PmmlNode.mkPmmlFieldColumnPair(namespaceURI, localName, qName, atts)
			case "row" => PmmlNode.mkPmmlrow(namespaceURI, localName, qName, atts)
			case "TableLocator" => PmmlNode.mkPmmlTableLocator(namespaceURI, localName, qName, atts)
			case "InlineTable" => PmmlNode.mkPmmlInlineTable(namespaceURI, localName, qName, atts)
			case "RuleSetModel" => PmmlNode.mkPmmlRuleSetModel(namespaceURI, localName, qName, atts)
			case "MiningField" => PmmlNode.mkPmmlMiningField(namespaceURI, localName, qName, atts)
			case "MiningSchema" => PmmlNode.mkPmmlMiningSchema(namespaceURI, localName, qName, atts)
			case "SimpleRule" => PmmlNode.mkPmmlSimpleRule(namespaceURI, localName, qName, atts)
			case "ScoreDistribution" => PmmlNode.mkPmmlScoreDistribution(namespaceURI, localName, qName, atts)
			case "RuleSet" => PmmlNode.mkPmmlRuleSet(namespaceURI, localName, qName, atts)
			case "RuleSelectionMethod" => PmmlNode.mkPmmlRuleSelectionMethod(namespaceURI, localName, qName, atts)
			case "Array" => PmmlNode.mkPmmlArray(namespaceURI, localName, qName, atts)
			case "SimplePredicate" => PmmlNode.mkPmmlSimplePredicate(namespaceURI, localName, qName, atts)
			case "CompoundPredicate" => PmmlNode.mkPmmlCompoundPredicate(namespaceURI, localName, qName, atts)
			//case _ => new PmmlNode(namespaceURI, localName, qName)
		}
		if (node != null) {
			/** update the parent on the stack if appropriate */
			if (! ctx.pmmlNodeStack.isEmpty) {
				ctx.pmmlNodeStack.top.addChild(node)
			}
			
			/** push the newly established node to the stack */
			ctx.pmmlNodeStack.push(node)
		}
	}
		
	
	/** Dispatcher for semantic analysis */
	def dispatchPmmlToPmmlExecXform(xformer : PmmlXform, qName : String, currentNode : PmmlNode) {

		var node : Option[PmmlExecNode] = qName match {
			case "Constant" => PmmlExecNode.mkPmmlExecConstant(this, currentNode.asInstanceOf[PmmlConstant])
			case "Header" => PmmlExecNode.mkPmmlExecHeader(this, currentNode.asInstanceOf[PmmlHeader])
			case "Application" => PmmlExecNode.mkPmmlExecApplication(this, currentNode.asInstanceOf[PmmlApplication])
			case "DataDictionary" => PmmlExecNode.mkPmmlExecDataDictionary(this, currentNode.asInstanceOf[PmmlDataDictionary])
			case "DataField" => PmmlExecNode.mkPmmlExecDataField(this, currentNode.asInstanceOf[PmmlDataField])
			case "Value" => PmmlExecNode.mkPmmlExecValue(this, currentNode.asInstanceOf[PmmlValue])
			case "Interval" => PmmlExecNode.mkPmmlExecInterval(this, currentNode.asInstanceOf[PmmlInterval])
			case "TransformationDictionary" => PmmlExecNode.mkPmmlExecTransformationDictionary(this, currentNode.asInstanceOf[PmmlTransformationDictionary])
			case "DerivedField" => PmmlExecNode.mkPmmlExecDerivedField(this, currentNode.asInstanceOf[PmmlDerivedField])
			case "DefineFunction" => PmmlExecNode.mkPmmlExecDefineFunction(this, currentNode.asInstanceOf[PmmlDefineFunction])
			case "ParameterField" => PmmlExecNode.mkPmmlExecParameterField(this, currentNode.asInstanceOf[PmmlParameterField])
			case "Apply" => PmmlExecNode.mkPmmlExecApply(this, currentNode.asInstanceOf[PmmlApply])
			case "FieldRef" => PmmlExecNode.mkPmmlExecFieldRef(this, currentNode.asInstanceOf[PmmlFieldRef])
			case "MapValues" => PmmlExecNode.mkPmmlExecMapValues(this, currentNode.asInstanceOf[PmmlMapValues])
			case "FieldColumnPair" => PmmlExecNode.mkPmmlExecFieldColumnPair(this, currentNode.asInstanceOf[PmmlFieldColumnPair])
			case "row" => PmmlExecNode.mkPmmlExecrow(this, currentNode.asInstanceOf[Pmmlrow])
			case "TableLocator" => PmmlExecNode.mkPmmlExecTableLocator(this, currentNode.asInstanceOf[PmmlTableLocator])
			case "InlineTable" => PmmlExecNode.mkPmmlExecInlineTable(this, currentNode.asInstanceOf[PmmlInlineTable])
			case "RuleSetModel" => PmmlExecNode.mkPmmlExecRuleSetModel(this, currentNode.asInstanceOf[PmmlRuleSetModel])
			case "MiningSchema" => PmmlExecNode.mkPmmlExecMiningSchema(this, currentNode.asInstanceOf[PmmlMiningSchema])
			case "MiningField" => PmmlExecNode.mkPmmlExecMiningField(this, currentNode.asInstanceOf[PmmlMiningField])
			case "SimpleRule" => PmmlExecNode.mkPmmlExecSimpleRule(this, currentNode.asInstanceOf[PmmlSimpleRule])
			case "ScoreDistribution" => PmmlExecNode.mkPmmlExecScoreDistribution(this, currentNode.asInstanceOf[PmmlScoreDistribution])
			case "RuleSet" => PmmlExecNode.mkPmmlExecRuleSet(this, currentNode.asInstanceOf[PmmlRuleSet])
			case "RuleSelectionMethod" => PmmlExecNode.mkPmmlExecRuleSelectionMethod(this, currentNode.asInstanceOf[PmmlRuleSelectionMethod])
			case "Array" => PmmlExecNode.mkPmmlExecArray(this, currentNode.asInstanceOf[PmmlArray])
			case "SimplePredicate" => PmmlExecNode.mkPmmlExecSimplePredicate(this, currentNode.asInstanceOf[PmmlSimplePredicate])
			case "CompoundPredicate" => PmmlExecNode.mkPmmlExecCompoundPredicate(this, currentNode.asInstanceOf[PmmlCompoundPredicate])
			case _ => None
		}

		node match {
			case Some(node) => {		  
				/** update the parent on the stack if appropriate */
				if (! pmmlExecNodeStack.isEmpty) {				  
					val top : Option[PmmlExecNode] = pmmlExecNodeStack.top
					top match {
					  	case Some(top) => {
						  	var parent : PmmlExecNode = top.asInstanceOf[PmmlExecNode]
							parent.addChild(node)
					  	}
					  	case _ => logger.error("Fantastic... there are None elements on the stack!")
					}
				}
				pmmlExecNodeStack.push(Some(node))
				val aNewNode : PmmlExecNode = node
				currentNode.Children.foreach((child) => {
					xformer.transform1(child.asInstanceOf[PmmlNode])
				})
				val completedXformNode : Option[PmmlExecNode] = pmmlExecNodeStack.pop().asInstanceOf[Option[PmmlExecNode]]
				completedXformNode match {
					case Some(completedXformNode) => {
						if (topLevelContainers.contains(completedXformNode.qName)) {
							pmmlExecNodeMap(completedXformNode.qName) = Some(completedXformNode)
						}
					}
					case _ => { /** comment out this case once tested */
						 logger.debug(s"node $currentNode.qName does not have children or had children subsumed by the parent")
					}
				}
			}
			case _ => {
				logger.debug(s"node $currentNode make did not produce a PmmlExecNode derivative")
			}
		}
	}
	
	/** Collect the udf function and indirectly their ParameterFields from the TransformationDictionary.
	 *  Note: we may want to go through all of the nodes to collect these ...for now just the TransformationDictionary.
	 */
	def collectUdfs()  {
	    val udfCollector : UdfCollector = new UdfCollector(this)
		val xDictNode : Option[PmmlExecNode] = pmmlExecNodeMap.apply("TransformationDictionary")
		PmmlExecNodeVisitor.Visit(xDictNode, udfCollector)
	}

	/** Collect the input variables required to produce the ModelDef */
	def collectModelInputVars : Unit =  {
	    val inVarCollector : ContainerFieldRefCollector = new ContainerFieldRefCollector(this)
		val dDictNode : Option[PmmlExecNode] = pmmlExecNodeMap.apply("DataDictionary")
		PmmlExecNodeVisitor.Visit(dDictNode, inVarCollector)
		val xDictNode : Option[PmmlExecNode] = pmmlExecNodeMap.apply("TransformationDictionary")
		PmmlExecNodeVisitor.Visit(xDictNode, inVarCollector)
	}
	
	def collectModelOutputVars : Unit = {
		/** filter for predicted and supplementary fields */
		val outputMiningFields : Array[xMiningField] = 
				miningSchemaMap.values.filter( fld => fld.usageType == "predicted" || fld.usageType == "supplementary").toArray
		val expandCompoundFieldTypes : Boolean = true
		outputMiningFields.foreach(fld => { 
			val name = fld.name
			
			val typeInfo : Array[(String,Boolean,BaseTypeDef)] = getFieldType(name, expandCompoundFieldTypes)
			if (typeInfo == null || (typeInfo != null && typeInfo.size == 0) || (typeInfo != null && typeInfo.size > 0 && typeInfo(0)._3 == null)) {
				//throw new RuntimeException(s"collectModelOutputVars: the mining field $name does not refer to a valid field in one of the dictionaries")
				logger.error(s"mining field named '$name' does not exist... your model is going to fail... however, let's see how far we can go to find any other issues...")
				modelOutputs(name.toLowerCase()) = (name, "is a bad mining variable referring to no valid dictionary item", "UNKNOWN TYPE")
			} else {
			
				/** FIXME: if a container is present (typeInfo.size > 1), should the container itself be added to the list of output
				 	vars instead of the field?  I think so... otoh, there is likely just one row out of this message container.. can
				 	the engine pull that row given the information provided.  Shouldn't there be a key emitted to select that row?  Or? */
				if (typeInfo.size > 1) {
					/** ... write them both for now and bring it up tomorrow */
					val (containerType, isContainer, containerBaseTypeDef) = typeInfo(0)
					val (fldType, isFldAContainer, fieldBaseTypeDef) = typeInfo(1)
					val containerName : String = fld.name.split('.').head
					val containervalue = (containerName, containerBaseTypeDef.NameSpace, containerBaseTypeDef.typeString)
					/** give the full container.name for the field reference */
					val typeStr : String = fieldBaseTypeDef.typeString
					val fldTypeName : String = fieldBaseTypeDef.Name
					val fldTypeNameSp : String = fieldBaseTypeDef.NameSpace
					val fldvalue = (name, fldTypeNameSp, fldType)
					//modelOutputs(containerName.toLowerCase()) = containervalue
					modelOutputs(name.toLowerCase()) = fldvalue
				} else {
					val (fldType, isFldAContainer, fieldBaseTypeDef) = typeInfo(0)
					val typeStr : String = fldType
					val fldTypeName : String = fieldBaseTypeDef.Name
					val fldTypeNameSp : String = fieldBaseTypeDef.NameSpace
					val fldvalue = (name, fldTypeNameSp, fldTypeName)
					modelOutputs(name.toLowerCase()) = fldvalue
				}
			}
			
		})
	}

	
	/** 
	 *  Collect the categorized values from the top level apply functions (i.e., parent node is xDerivedField).
	 *  Update the top level apply functions categorized value array with them, eliminating them from the child
	 *  nodes of the xDerived field.
	 */
	def transformTopLevelApplyNodes()  {
	    val catTransformer : IfActionTransform = new IfActionTransform(this)
		val xDictNode : Option[PmmlExecNode] = pmmlExecNodeMap.apply("TransformationDictionary")
		PmmlExecNodeVisitor.Visit(xDictNode, catTransformer)
	}
		
	def ruleSetModelInfoCollector()  {
	    val rsModelCollector : RuleSetModelCollector = new RuleSetModelCollector(this)
	    val rsm : Option[PmmlExecNode] = pmmlExecNodeMap.apply("RuleSetModel") 
		PmmlExecNodeVisitor.Visit(rsm, rsModelCollector)
	}

	def simpleRuleInfoCollector()  {
	    val rsModelCollector : SimpleRuleCollector = new SimpleRuleCollector(this)
	    val rsm : Option[PmmlExecNode] = pmmlExecNodeMap.apply("RuleSetModel") 
		PmmlExecNodeVisitor.Visit(rsm, rsModelCollector)
	}
	
	def UdfReturnType(udfName : String) : String = {
		val returnType : String = if (udfMap.contains(udfName)) {
			val udf : xDefineFunction = udfMap.apply(udfName).asInstanceOf[xDefineFunction]
			udf.dataType
		} else {
			"Any"
		}
		returnType
	}
	
	/** Answer the pmml dataTypes for the udf with the supplied name as an ArrayBuffer */
	def UdfParameterTypes(udfName : String) : ArrayBuffer[String] = {
	  
		var parameterTypes : ArrayBuffer[String] = new ArrayBuffer[String]()
		if (udfMap.contains(udfName)) {
			val udf : xDefineFunction = udfMap.apply(udfName).asInstanceOf[xDefineFunction]
		  	udf.Children.foreach((child) => {
				child match {
				  case p : xParameterField => parameterTypes += p.dataType
				  case _ => None
				}
		  	})
		}
		parameterTypes
	}
	
	/** Answer the pmml arg names for the udf with the supplied name as an ArrayBuffer */
	def UdfParameterNames(udfName : String) : ArrayBuffer[String] = {
	  
		var parameterNames : ArrayBuffer[String] = new ArrayBuffer[String]()
		if (udfMap.contains(udfName)) {
			val udf : xDefineFunction = udfMap.apply(udfName).asInstanceOf[xDefineFunction]
		  	udf.Children.foreach((child) => {
				child match {
				  case p : xParameterField => parameterNames += p.name
				  case _ => None
				}
		  	})
		}
		parameterNames
	}
	

}

