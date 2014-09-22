package com.ligadata.Compiler

import scala.collection.mutable._
import scala.collection.immutable.{ Set }
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._


class MetadataInterpreter(val ctx : PmmlContext) extends LogTrait {
  
	val mgr : MdMgr = ctx.mgr

	/** 
	 *  Answer min/max count of arguments for the functions with the supplied name 
	 *  @param name a namespace.name string key.
	 */
	def minMaxArgsForTheseFcns(name : String) : (Int,Int) = {
		var argCntMin = 1000000
		var argCntMax = 0
	  	val availableFcns : Set[FunctionDef] = getFunctions(name)
	  	if (availableFcns != null) {
	  		availableFcns.foreach( f => {
	  			argCntMin = if (f.args.size > argCntMin) f.args.size else argCntMin 
	  			argCntMax = if (f.args.size < argCntMax) f.args.size else argCntMax 
	  		})
	  	}
	  	argCntMax = if (argCntMax == 1000000) 0 else argCntMax
	  	(argCntMin,argCntMax) 
	}
	
	
	/** 
	 *  Answer an Array[(String,Boolean,BaseTypeDef)] that describe the types in a nested container field reference.
	 *  If expandCompoundFieldTypes is true, all types for each container and final leaf field element are returned in
	 *  the array.  If false, only the field's type is returned.
	 *  
	 *  @param names - an array of the string components from a (possibly) compound field reference expression
	 *  @param expandCompoundFieldTypes - when true, the full type information for each type in the field expression
	 *  	are returned.  When false, only the leaf type information is returned.
	 *  @return an array of triples consisting of (type string representation, isContainerWithNamedValues, the BaseTypeDef)
	 */
	
	def getFieldType(names : Array[String], expandCompoundFieldTypes : Boolean) : Array[(String,Boolean,BaseTypeDef)] = {	
		var baseTypeTriples : ArrayBuffer[(String,Boolean,BaseTypeDef)] = ArrayBuffer[(String,Boolean,BaseTypeDef)]()
		val buffer = new StringBuilder
		names.addString(buffer, ".")
		val fldRefExprStr : String = buffer.toString
		val totalNames : Int = names.size

		getFieldType(names, fldRefExprStr, totalNames, null, baseTypeTriples)
		
		val returnArray : Array[(String,Boolean,BaseTypeDef)] = if (! expandCompoundFieldTypes) {
			Array[(String,Boolean,BaseTypeDef)](baseTypeTriples.last)
		} else {
			baseTypeTriples.toArray
		}
		returnArray	
	}

	/** 
	 *  Collect an array buffer of(String,Boolean,BaseTypeDef) that describe the types in a nested container field 
	 *  reference (e.g., container.subcontainer.field)
	 *  
	 *  @param names - the names in a container.subcontainer.subsubcontainer....field reference expression that remain
	 *  				to be examined.
	 *  @param fieldRefRep - the original field ref as a string rep for diagnostics
	 *  @param totalNames - the count of container/field names in the original field ref expression
	 *  @param parentContainer - except for the first container in a nested field reference, this parameter contains
	 *  		the immediate preceding type reference derived from the prior name.  It is used to lookup the current name.
	 *  @param baseTypeTriples - this array buffer is updated with the triples.
	 *  
	 */
	
	def getFieldType(names : Array[String]
					, fieldRefRep : String
					, totalNames : Int
					, parentContainer : ContainerTypeDef
					, baseTypeTriples : ArrayBuffer[(String,Boolean,BaseTypeDef)]) : Unit = {	
	  	var priorType : BaseTypeDef = null
	  	var name : String = null
	  	if (names != null && names.size > 0) {
			name = names.head
			/** The variable is either defined in one of the dictionaries (ddict or xdict) or is a field (or key) inside
			 *  one of these containers  */
			if (name != null) {
				if (ctx.dDict.contains(name) && parentContainer == null) {
					val fld : xDataField = ctx.dDict.apply(name)
					val containerType : ContainerTypeDef = getContainerWithNamesType(fld.dataType)
					if (containerType == null && names.size != 1) { /** this is not a container and therefore should be last name */
						logger.error(s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
						logger.error(s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")
					} else {
						/** it may indeed be the last one, but let's see if there is more */
						if (containerType == null) {
							if (totalNames == 1) {
								val conceptCollected : Boolean = collectConceptType(name, baseTypeTriples)
								if (! conceptCollected) {
									val (typestring,typedef) : (String,BaseTypeDef) = getDDictFieldType(name)
									if (typedef == null) {
										logger.trace(s"The field type, '${fld.dataType} for data dictionary field '$name' is not found")
										logger.trace("Either correct the spelling or add a type with this name to the metadata")
									} else {
										baseTypeTriples += Tuple3(typestring, (containerType == null), typedef)
									}
								}
							} else {
								val parentsName : String = if (parentContainer != null) parentContainer.Name else "(no parent name avaialable)"
								/** embarrassing situation... there is a logic error in this code... fix it */
								logger.error(s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
								logger.error(s"If there is a parent name, then there is a logic error. Contact us.")
							}
						} else {
							/** this is a container */
							priorType = containerType /** remember this fact for recurse at bottom of function */
							val isContainerWithNamedFields = (containerType.isInstanceOf[StructTypeDef] || containerType.isInstanceOf[MappedMsgTypeDef])
							baseTypeTriples += Tuple3(priorType.typeString, isContainerWithNamedFields, priorType)
						}
					}
			  
				} else {
					val inXDict : Boolean = (ctx.xDict.contains(name)  && parentContainer == null)
					if (inXDict) {
						val fld : xDerivedField = ctx.xDict.apply(name)
						val containerType : ContainerTypeDef = getContainerWithNamesType(fld.dataType)
						if (containerType == null && names.size != 1) { /** this is not a container and therefore should be last name */
							logger.error(s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
							logger.error(s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")
						} else {
							/** it may indeed be the last one, but let's see if there is more */
							if (containerType == null) {
								if (totalNames == 1) {
									val conceptCollected : Boolean = collectConceptType(name, baseTypeTriples)
									if (! conceptCollected) {
										val (typestring,typedef) : (String,BaseTypeDef) = getXDictFieldType(name)
										if (typedef == null) {
											logger.trace(s"The field type, '${fld.dataType} for data dictionary field '$name' is not found")
											logger.trace("Either correct the spelling or add a type with this name to the metadata")
										} else {
											baseTypeTriples += Tuple3(typestring, (containerType == null), typedef)
										}
									}
								} else {
									val parentsName : String = if (parentContainer != null) parentContainer.Name else "(no parent name avaialable)"
									/** embarrassing situation... there is a logic error in this code... fix it */
									logger.error(s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
									logger.error(s"If there is a parent name, then there is a logic error. Contact us.")
								}
							} else {
								/** this is a container */
								priorType = containerType /** remember this fact for recurse at bottom of function */
								val isContainerWithNamedFields = (containerType.isInstanceOf[StructTypeDef] || containerType.isInstanceOf[MappedMsgTypeDef])
								baseTypeTriples += Tuple3(priorType.typeString, isContainerWithNamedFields, priorType)
							}
						}
					} else {
						/** this field name is not in the pmml dictionaries... search the parent container for it, if appropriate, else
						 *  record the field type information for it, assuming it is a valid base concept (explicitly cataloged attribute) 
						 *  or possibly a simple scalar */
						if (parentContainer != null) {						
							val attr : BaseAttributeDef = parentContainer match {
							  case s : StructTypeDef => parentContainer.asInstanceOf[StructTypeDef].attributeFor(name)
							  case m : MappedMsgTypeDef => parentContainer.asInstanceOf[MappedMsgTypeDef].attributeFor(name)
							  case _ => null
							}
							if (attr != null) {
								priorType = attr.typeDef
								val isContainerWithNamedFields = (priorType.isInstanceOf[StructTypeDef] || priorType.isInstanceOf[MappedMsgTypeDef])
								baseTypeTriples += Tuple3(priorType.typeString, isContainerWithNamedFields, priorType)
							} else {
								val parentsName : String = if (parentContainer != null) parentContainer.Name else "NO PARENT"
								logger.error(s"The name '$name' in field reference value '$fieldRefRep' could not be found in parent container '$parentsName'. ")
								logger.error(s"Either the name is incorrect or this field should be added as an attribute to the $parentsName.")
							}
						} else {
							/** This may be the sole element in the field reference and a public concept... let's see */
							if (totalNames == 1) {
								val conceptCollected : Boolean = collectConceptType(name, baseTypeTriples)
								if (! conceptCollected) {
									/** consider that this may be a simple type ... one last search in active types */
									val wtf : Boolean = true
								}
							} else {
								val parentsName : String = if (parentContainer != null) parentContainer.Name else "(no parent name avaialable)"
								/** embarrassing situation... there is a logic error in this code... fix it */
								logger.error(s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
								logger.error(s"If there is a parent name, then there is a logic error. Contact us.")
							}
						}
					}	  
				}
			}
		}
	  	if (names.size > 1) {
	  		val newParentContainer = if (priorType.isInstanceOf[ContainerTypeDef]) priorType.asInstanceOf[ContainerTypeDef] else null
	  		if (newParentContainer == null) {
				logger.error(s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
				logger.error(s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")	  
	  		} else {
		  		/** discard the just processed name and recurse to interpret remaining names... */
	  			val restOfNames : Array[String] = names.tail
		  		getFieldType(restOfNames, fieldRefRep, totalNames, newParentContainer, baseTypeTriples)
	  		}
	  	}
	}
			
	/** 
	 *  Answer a ContainerTypeDef that has names (either fields or known keys) or null 
	 *  if not a container.  Namespaces from the namespace
	 *  search path are used until the container is found or they are exhausted.
	 */
	def getContainerWithNamesType(name : String) : ContainerTypeDef = {
	
	  	var containerType : ContainerTypeDef = null
	  	breakable {
			ctx.namespaceSearchPath.foreach( nmspc => {			
				val cntnr : ContainerTypeDef = getContainerWithNamesType(nmspc, name)
				if (cntnr != null) {
					containerType = cntnr
					break
				}
			})
	  	}
	  	containerType
	}
	
	/** 
	 *  Answer a ContainerTypeDef that has named fields or null if not a container.  Two types currently
	 *  have this characteristic: StructTypeDef and MappedMsgTypeDef
	 *  @param nmSpc - the name of a possible container
	 *  @param container - the name of a possible container
	 *  @return the first ContainerTypeDef associated with this name or null if not found
	 */
	def getContainerWithNamesType(nmSpc : String, container : String) : ContainerTypeDef = {
		val baseTypeDef : BaseTypeDef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, container))
		val containerTypeDef : ContainerTypeDef = baseTypeDef match {
		  case s : StructTypeDef => baseTypeDef.asInstanceOf[StructTypeDef]
		  case m : MappedMsgTypeDef => baseTypeDef.asInstanceOf[MappedMsgTypeDef]
		  case _ => null
		}

	  	containerTypeDef
	}

	/** 
	 *  Answer a ContainerTypeDef or null if not a container.  Namespaces from the namespace
	 *  search path are used until the container is found or they are exhausted.
	 *  
	 *  NOTE: This function will return any kind of ContainerTypeDef, not just those with
	 *  known names.  Included types would be class ListTypeDef, QueueTypeDef, ArrayTypeDef
	 *  ArrayBufTypeDef, TupleTypeDef, among others.
	 *  
	 *  @param name - the name of a possible container
	 *  @return the first ContainerTypeDef associated with this name or null if not found
	 */
	def getContainerType(name : String) : ContainerTypeDef = {
	
	  	var containerType : ContainerTypeDef = null
	  	breakable {
			ctx.namespaceSearchPath.foreach( nmspc => {			
				val cntnr : ContainerTypeDef = getContainerType(nmspc, name)
				if (cntnr != null) {
					containerType = cntnr
					break
				}
			})
	  	}
	  	containerType
	}

	
	/** 
	 *  Answer a ContainerTypeDef or null if not a container
	 */
	def getContainerType(nmSpc : String, container : String) : ContainerTypeDef = {
		val baseTypeDef : BaseTypeDef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, container))
		val containerTypeDef = baseTypeDef match {
		  case c : ContainerTypeDef => baseTypeDef.asInstanceOf[ContainerTypeDef]
		  case _ => null
		}

	  	containerTypeDef
	}

	/** 
	 *  Answer a BaseAttributeDef or null for the supplied concept name.  Namespaces from the namespace
	 *  search path are used until the concept is found or they are exhausted.
	 */
	def getConcept(name : String) : BaseAttributeDef = {
	
	  	var attr : BaseAttributeDef = null
	  	breakable {
			ctx.namespaceSearchPath.foreach( nmspc => {			
				attr  = mgr.ActiveAttribute(nmspc, name)
				if (attr != null) {
					break
				}
			})
	  	}
	  	attr
	}

	/** 
	 *  Answer a BaseTypeDef or null for the supplied concept name.  Namespaces from the namespace
	 *  search path are used until the concept is found or they are exhausted.
	 */
	def getConceptType(name : String) : BaseTypeDef = {
		val attr : BaseAttributeDef = getConcept(name)
	  	val attrType : BaseTypeDef = if (attr != null) attr.typeDef else null
	  	attrType
	}

	/** 
	 *  Take a flier... assume it is a base concept (an independently cataloged attribute).  Collect it if 
	 *  you can and report success/failure of this effort.
	 */
	def collectConceptType(name : String, baseTypeTriples : ArrayBuffer[(String,Boolean,BaseTypeDef)]) : Boolean = {
		val conceptType : BaseTypeDef = getConceptType(name)
		val gotOne = if (conceptType != null) {
			val isContainerWithNamedFields = (conceptType.isInstanceOf[StructTypeDef] || conceptType.isInstanceOf[MappedMsgTypeDef])
			baseTypeTriples += Tuple3(conceptType.typeString, isContainerWithNamedFields, conceptType)
			true
		} else {
			//logger.trace(s"The name '$name' in field reference value is not a valid concept. ")
			false
		}
		gotOne
	}

	/**
	 * Find the Set[FunctionDef] for the function name supplied.  Function names may be qualified with a namespace
	 * if desired.  If not qualified this way, the namespaceSearchPath is used to supply the namespace for the 
	 * search.  Each is searched in order until either the namespace.fcnName matches or all namespace.fcnName's 
	 * have been used and the search fails.
	 * 
	 * @param fcnName the name, possibly "namespace." qualified key 
	 * @return FunctionDef
	 */
	def getFunctions(fcnName : String) : Set[FunctionDef] = {

		var functions : Set[FunctionDef] = Set[FunctionDef]()
		var nmSpcFunctions : Set[FunctionDef] = null
		if (fcnName != null && fcnName.size > 0) {	  
			val nameParts : Array[String] = if (fcnName.contains(".")) {
				fcnName.split('.')
			} else {
				Array(fcnName)
			}
			if (nameParts.size > 2) { /** we won't use any more than two  ... no nested namespaces */
				logger.error("There are more namespaces than can be used... getFunctions fails due to bad argument $fcnName")
			} else {
				if (nameParts.size == 2) {  /** asking for specific namespace */
					val nmspFcns : Set[FunctionDef] = mgr.FunctionsAvailable(nameParts(0),nameParts(1))
					if (functions != null && nmspFcns != null && nmspFcns.size > 0) { 
						functions ++= nmspFcns
					}
				} else { /** just one unqualified name */
					/** use the namespaceSearchPath elements for the namespace until a match or all possibilities exhausted */

					ctx.namespaceSearchPath.foreach( nmspc => {			
						val nmspFcns : Set[FunctionDef] = mgr.FunctionsAvailable(nmspc,fcnName)
						if (functions != null && nmspFcns != null && nmspFcns.size > 0) {
						  functions ++= nmspFcns
						}
					})
				}
			}
		}
		functions
	}
	
	def FunctionByTypeSig(typesig : String) : FunctionDef = {
		var fdef : FunctionDef = null
		val buffer : StringBuilder = new StringBuilder
		breakable {
		  	ctx.namespaceSearchPath.foreach( path => {
		  		buffer.clear
		  		buffer.append(path)
		  		buffer.append(".")
		  		buffer.append(typesig)
		  		val key = buffer.toString
		  		fdef = mgr.FunctionByTypeSig(key)
		  		if (fdef != null) {
		  			break
		  		}
		  	})
		}
	  	fdef
	}
	
	/** The macro search is always without namespace prefix ... */
	def MacroByTypeSig(typesig : String) : MacroDef = {
		var mdef : MacroDef = null
		val buffer : StringBuilder = new StringBuilder
		breakable {
		  	ctx.namespaceSearchPath.foreach( path => {
		  		buffer.clear
		  		buffer.append(path)
		  		buffer.append(".")
		  		buffer.append(typesig)
		  		val key = buffer.toString
		  		mdef = mgr.MacroByTypeSig(key)
		  		if (mdef != null) {
		  			break
		  		}
		  	})
		}

	  	mdef
	}
	
	/** Search this bad boy, with and without namespace qualifiers...
	 *  @param nm the name of the data dictionary data field
	 *  @return a tuple (type rep string , corresponding base type) 
	 */
	def getDDictFieldType(nm : String) : (String,BaseTypeDef) = {
		var typeStr : String = null
		var baseType : BaseTypeDef = null
		val withNmSpc : Boolean = true
		breakable {
		  	ctx.namespaceSearchPath.foreach( nmSpc => {
		  		val (typeStrWOut, baseTypeWOut) = getDDictFieldType(nmSpc, nm : String, ! withNmSpc)
		  		if (baseTypeWOut != null) {
					typeStr = typeStrWOut
					baseType = baseTypeWOut
		  			break
		  		}
		  		val (typeStrWith, baseTypeWith) = getDDictFieldType(nmSpc, nm : String, withNmSpc)
		  		if (baseTypeWith != null) {
					typeStr = typeStrWith
					baseType = baseTypeWith
		  			break
		  		}
		  	})
		}

	  	(typeStr, baseType)
	}
	
	/** Search this bad boy, with and without namespace qualifiers...
	 *  @param nmSpc the namespace key to use for the search when withNmSpc arg set
	 *  @param nm the name of the data dictionary field
	 *  @param withNmSpc the namespace is used to prefix the dataType name
	 *  @return a tuple (type rep string , corresponding base type) 
	 */
	private def getDDictFieldType(nmSpc : String, nm : String, withNmSpc : Boolean) : (String,BaseTypeDef) = {	
		val key : String = if (withNmSpc) nmSpc + "." + nm else nm
		var fldTypedef : BaseTypeDef = null
		val scalaType : String = if (ctx.dDict.contains(key)) {
			val fld : xDataField = ctx.dDict.apply(key)
			val fldType = fld.dataType
			fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, fldType)).asInstanceOf[BaseTypeDef]
			if (fldTypedef != null) {
				fldTypedef.typeString
			} else {
				/** Translate the possible builtin type to a scala equiv and try again. */
				val scalaVersionOfPmmlBuiltinType : String = PmmlTypes.scalaDataType(fldType)
				/** verify it is present in the metadata and get metadata description for it */
				fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, scalaVersionOfPmmlBuiltinType))
				fldTypedef.typeString
			}
		} else null 
		(scalaType,fldTypedef)
	}

	/** Search this bad boy, with and without namespace qualifiers...
	 *  @param nm the name of the transformation dictionary derived field
	 *  @return a tuple (type rep string , corresponding base type) 
	 */
	def getXDictFieldType(nm : String) : (String,BaseTypeDef) = {
		var typeStr : String = null
		var baseType : BaseTypeDef = null
		val withNmSpc : Boolean = true
		breakable {
		  	ctx.namespaceSearchPath.foreach( nmSpc => {
		  		val (typeStrWOut, baseTypeWOut) = getXDictFieldType(nmSpc, nm : String, ! withNmSpc)
		  		if (baseTypeWOut != null) {
					typeStr = typeStrWOut
					baseType = baseTypeWOut
		  			break
		  		}
		  		val (typeStrWith, baseTypeWith) = getXDictFieldType(nmSpc, nm : String, withNmSpc)
		  		if (baseTypeWith != null) {
					typeStr = typeStrWith
					baseType = baseTypeWith
		  			break
		  		}
		  	})
		}

	  	(typeStr, baseType)
	}
	
	/** Search this bad boy, with and without namespace qualifiers...
	 *  @param nmSpc the namespace key to use for the search when withNmSpc arg set
	 *  @param nm the name of the transformation dictionary derived field
	 *  @param withNmSpc the namespace is used to prefix the dataType name
	 *  @return (typename string, typedef)
	 */
	private def getXDictFieldType(nmSpc : String, nm : String, withNmSpc : Boolean) : (String,BaseTypeDef) = {	
		val key : String = if (withNmSpc) nmSpc + "." + nm else nm
		var fldTypedef : BaseTypeDef = null
		val scalaType : String = if (ctx.xDict.contains(key)) {
			val fld : xDerivedField = ctx.xDict.apply(key)
			val fldType = fld.dataType
			fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, fldType)).asInstanceOf[BaseTypeDef]
			if (fldTypedef != null) {
				fldTypedef.typeString
			} else {
				/** Translate the possible builtin type to a scala equiv and try again. */
				val scalaVersionOfPmmlBuiltinType : String = PmmlTypes.scalaDataType(fldType)
				/** verify it is present in the metadata and get metadata description for it */
				fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, scalaVersionOfPmmlBuiltinType))
				fldTypedef.typeString
			}
		} else null 
		(scalaType,fldTypedef)
	}


	/** Answer the typedef string and typedef for the supplied typename.
	 *  @param typename the name of the transformation dictionary derived field
	 *  @return (type name string, typedef)
	 */
	def getType(typename : String) : (String,BaseTypeDef) = {
		var typeStr : String = null
		var baseType : BaseTypeDef = null
		breakable {
		  	ctx.namespaceSearchPath.foreach( nmSpc => {
		  		val fldTypedef : BaseTypeDef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, typename)).asInstanceOf[BaseTypeDef]
				if (fldTypedef != null) {
					typeStr = fldTypedef.typeString
					baseType = fldTypedef
					break
				} else {
					/** Translate the possible builtin type to a scala equiv and try again. */
					val scalaVersionOfPmmlBuiltinType : String = PmmlTypes.scalaDataType(typename)
					/** verify it is present in the metadata and get metadata description for it */
					val fldTypedef = mgr.ActiveType(MdMgr.MkFullName(nmSpc, scalaVersionOfPmmlBuiltinType))
					if (fldTypedef != null) {
						 typeStr = fldTypedef.typeString
						 baseType = fldTypedef
						 break
					}
				}
		  	})
		}

	  	(typeStr, baseType)	
	}
	
	/** 
	 *  Answer whether this is a container that has a list keys or fields
	 *  @param typedef A BaseTypeDef
	 *  @return true if typedef is any {StructTypeDef, MappedMsgTypeDef}
	 */
	def isContainerWithFieldOrKeyNames(typedef : BaseTypeDef) : Boolean = {
	  	(typedef != null && (typedef.isInstanceOf[StructTypeDef] || typedef.isInstanceOf[MappedMsgTypeDef]))
	}

}


