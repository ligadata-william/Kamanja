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
		
		val returnArray : Array[(String,Boolean,BaseTypeDef)] = if (! expandCompoundFieldTypes ) {
			if (baseTypeTriples.size > 0) {
				Array[(String,Boolean,BaseTypeDef)](baseTypeTriples.last)
			} else {
				Array[(String,Boolean,BaseTypeDef)]()
			}
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
						PmmlError.logError(ctx, s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
						PmmlError.logError(ctx, s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")
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
										baseTypeTriples += Tuple3(typestring, (containerType != null), typedef)
									}
								}
							} else {
								val parentsName : String = if (parentContainer != null) parentContainer.Name else "(no parent name avaialable)"
								/** embarrassing situation... there is a logic error in this code... fix it */
								PmmlError.logError(ctx, s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
								PmmlError.logError(ctx, s"If there is a parent name, then there is a logic error. Contact us.")
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
							PmmlError.logError(ctx, s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
							PmmlError.logError(ctx, s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")
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
									PmmlError.logError(ctx, s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
									PmmlError.logError(ctx, s"If there is a parent name, then there is a logic error. Contact us.")
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
								PmmlError.logError(ctx, s"The name '$name' in field reference value '$fieldRefRep' could not be found in parent container '$parentsName'. ")
								PmmlError.logError(ctx, s"Either the name is incorrect or this field should be added as an attribute to the $parentsName.")
							}
						} else {
							/** This may be the sole element in the field reference and a public concept... let's see */
							if (totalNames == 1) {
								val conceptCollected : Boolean = collectConceptType(name, baseTypeTriples)
								if (! conceptCollected) {
									/** consider that this may be a simple type ... one last search in active types */
									val huh : String = "huh" // debugging rest stop 
								}
							} else {
								val parentsName : String = if (parentContainer != null) parentContainer.Name else "(no parent name avaialable)"
								/** embarrassing situation... there is a logic error in this code... fix it */
								PmmlError.logError(ctx, s"The name '$name' in '$fieldRefRep' was not found.  Its parent was '$parentsName'. ")
								PmmlError.logError(ctx, s"If there is a parent name, then there is a logic error. Contact us.")
							}
						}
					}	  
				}
			}
		}
	  	if (names.size > 1) {
	  		val newParentContainer = if (priorType.isInstanceOf[ContainerTypeDef]) priorType.asInstanceOf[ContainerTypeDef] else null
	  		if (newParentContainer == null) {
				PmmlError.logError(ctx, s"Malformed compound name... The name '$name' in field reference value '$fieldRefRep' is not a container. ")
				PmmlError.logError(ctx, s"Either the metadata type for this field is incorrect or it should be the last field in the field reference.")	  
	  		} else {
		  		/** discard the just processed name and recurse to interpret remaining names... */
	  			val restOfNames : Array[String] = names.tail
		  		getFieldType(restOfNames, fieldRefRep, totalNames, newParentContainer, baseTypeTriples)
	  		}
	  	}
	}
	
	/**
	 * This getFieldType variant uses the model information supplied to locate the derived concept (output variable from some model) and then
	 * form an array of triples consisting of (type name string, isContainer boolean, typedef declaration for this type) using the derivedFieldKey 
	 * and subFldKeys (if supplied). The subFldKeys name, when not null, is expected to be one or more field names delimited with '.'.  When 
	 * this field is null, only the derived concept type information is returned in the array of triple.
	 * 
	 * @param model the ModelDef that has the supplied derived field key in its output variables.
	 * @param derivedFieldKey the fully qualified key name of some published output variable from the supplied model.
	 * @param subFldKeys this contains a (possibly) '.' delimited reference to a field found in the container or one of the subcontainers
	 *		it contains ad infinitum
	 * @return an array of (typeNameStr, isContainer, typedef) tuples describing the type information for each node in subFldKeys from 
	 * 		outer type to terminal field type  
	 */
	def getFieldType(model : ModelDef, derivedFieldKey : String, subFldKeys  : String) : Array[(String,Boolean,BaseTypeDef)] = {
		var subFldKeyTypeInfo : ArrayBuffer[(String,Boolean,BaseTypeDef)] = ArrayBuffer[(String,Boolean,BaseTypeDef)]()
		val fullConceptKey : String = model.FullNameWithVer + "." + derivedFieldKey
		val (conceptTypeNamespace, conceptTypeName) : (String,String) = model.OutputVarType(fullConceptKey)
		if (conceptTypeNamespace != null && conceptTypeName != null) {
			val typedef : BaseTypeDef = ctx.mgr.ActiveType(conceptTypeNamespace, conceptTypeName)
			if (typedef != null) {
				val isContainer : Boolean = isContainerWithFieldOrKeyNames(typedef)
				val typeStr : String = typedef.typeString
				subFldKeyTypeInfo += Tuple3(typeStr, isContainer, typedef)
			} else {
				PmmlError.logError(ctx, s"Unable to locate the derived field $fullConceptKey's $conceptTypeNamespace.$conceptTypeName in the metadata")
			}
			if (subFldKeys != null) {
				var isContainer : Boolean = isContainerWithFieldOrKeyNames(typedef)
				var currContainerTypeName : String = conceptTypeName
				var currTypeDef : BaseTypeDef = typedef
				if (! isContainer) {
					PmmlError.logError(ctx, s"derived field $derivedFieldKey is not a container but was given field names...$subFldKeys ....")
				} else {
					val subFldKeyNodes : Array[String] = subFldKeys.split(".")
					subFldKeyNodes.foreach(node => {
						val attrdef : BaseAttributeDef = attributeFor(currTypeDef, node)
						if (attrdef != null) {
							currTypeDef = attrdef.typeDef
							currContainerTypeName = currTypeDef.Name
							isContainer = isContainerWithFieldOrKeyNames(currTypeDef)
							subFldKeyTypeInfo += Tuple3(currTypeDef.typeString, isContainer, currTypeDef)
						} else {
							PmmlError.logError(ctx, s"Bad attribute name supplied $node")
						}
					})
				}
			} 
		} else {
			PmmlError.logError(ctx, s"Unable to locate the derived field $fullConceptKey in the model ${model.FullNameWithVer}. A typo perhaps?")
		}
		 
		subFldKeyTypeInfo.toArray
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
	def getFunctions(fcnName : String) : scala.collection.immutable.Set[FunctionDef] = {

		var functions : Set[FunctionDef] = Set[FunctionDef]()
		var nmSpcFunctions : Set[FunctionDef] = null
		if (fcnName != null && fcnName.size > 0) {	  
			val nameParts : Array[String] = if (fcnName.contains(".")) {
				fcnName.split('.')
			} else {
				Array(fcnName)
			}
			if (nameParts.size > 2) { /** we won't use any more than two  ... no nested namespaces */
				PmmlError.logError(ctx, "There are more namespaces than can be used... getFunctions fails due to bad argument $fcnName")
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
	
	/** 
	 *  Answer any function definitions with the supplied namespace and name that have the indefinite arity feature.
	 *  
	 *  @param name : the name, possibly prefixed with a namespace, of this function.  With namespace, only it will be
	 *  	searched.  If no namespace is is specified, the namespace search path (part of the PmmlContext containing
	 *   	possibly multiple namespaces in a list) is used to
	 *   	for keys for the metadata search.
	 *  @return scala.collection.immutable.Set[FunctionDef] if any are present else null
	 */
	def FunctionsAvailableWithIndefiniteArity(fcnName : String) : scala.collection.immutable.Set[FunctionDef] = {
		val fcnsAvailable : scala.collection.immutable.Set[FunctionDef] = getFunctions(fcnName)
		val indefArityFcns : scala.collection.immutable.Set[FunctionDef] = if (fcnsAvailable != null) {
			fcnsAvailable.filter(fcn => fcn.features.contains(FcnMacroAttr.HAS_INDEFINITE_ARITY))
		} else {
		    null
		}
		indefArityFcns
  	}

	/** 
	 *  Answer if indefinite arity feature is present in any function with this name.
	 *  
	 *  @param name : the name, possibly prefixed with a namespace, of this function.  With namespace, only it will be
	 *  	searched.  If no namespace is is specified, the namespace search path (part of the PmmlContext containing
	 *   	possibly multiple namespaces in a list) is used to
	 *   	for keys for the metadata search.
	 *  @return Boolean
	 */
	def FunctionsWithIndefiniteArity(fcnName : String) : Boolean = {
		(FunctionsAvailableWithIndefiniteArity(fcnName).size > 0)
  	}

	/** 
	 *  With the supplied function signature, try to find the FunctionDef that matches it
	 *  
	 *  @paramn typesig : a function identifer with args in the form [namespace.]fcnName([argtype1,argtype2,...,argtypeN]) 
	 *  @return matching FunctionDef or null
	 */
	def FunctionByTypeSig(typesig : String) : FunctionDef = {
		var fdef : FunctionDef = null
		val buffer : StringBuilder = new StringBuilder
		breakable {
		  	/** if the namespace was supplied... allow it */
		  	if (typesig.split('(').head.contains(".")) {
		  		fdef = mgr.FunctionByTypeSig(typesig)
		  		if (fdef != null) {
		  			break
		  		}
		  	} else { /** prefix the sans namespace key with each namespace in the search path and try to find one */
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
		}
	  	fdef
	}
	
	/** 
	 *  With the supplied function signature, try to find the MacroByTypeSig that matches it
	 *  
	 *  @paramn typesig : a function identifer with args in the form [namespace.]fcnName([argtype1,argtype2,...,argtypeN]) 
	 *  @return matching MacroDef or null
	 *  
	 *  Note: The macro search is always without namespace prefix ...
	 */

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
	
	
	/** 
	 *  Answer the BaseAttributeDef for the supplied attribute name that is presumably part of the supplied container or message.
	 *  @param typedef A BaseTypeDef of a container or message that has named fields
	 *  @param attrName the name (presumably) of one of the StructTypeDef's or MappedMsgTypeDef's attributes
	 *  @return BaseAttributeDef for the supplied attrName or null if not present
	 */
	def attributeFor(typedef : BaseTypeDef, attrName : String) : BaseAttributeDef = {
	  	val attr : BaseAttributeDef = if (isContainerWithFieldOrKeyNames(typedef)) {
	  		if (typedef.isInstanceOf[StructTypeDef]) {
	  			typedef.asInstanceOf[StructTypeDef].attributeFor(attrName)
	  		} else {
	  			if (typedef.isInstanceOf[MappedMsgTypeDef]) {
	  				typedef.asInstanceOf[MappedMsgTypeDef].attributeFor(attrName)
	  			} else {
	  				PmmlError.logError(ctx, s"The supplied field name $attrName does not exist in container type ${typedef.FullNameWithVer} ")
	  				null
	  			}
	  		}
	  	} else {
			PmmlError.logError(ctx, s"The container type ${typedef.FullNameWithVer} is not a ContainerTypeDef with fieldNames.")
			null
	  	}
	  	if (attr == null) {
	  		PmmlError.logError(ctx, s"The supplied field name $attrName did not produce a BaseAttributeDef from container type ${typedef.FullNameWithVer}")
	  	}
	  	attr
	}
	
	/** Derived Field interpretation */
	
	/**
	 *  Is the supplied name prefixed with a known namespace?  Check the prefix node in the '.' delimited name, assuming it is present,
	 *	against the known namespaces in this model.  
	 * 
	 *  @param name a String that represents some derived field that potentially has one of the known namespaces as a prefix.
	 *  @return true if this name is prefixed with a namespace, else false
	 *   
	 */
	def HasNameSpacePrefix(name : String) : Boolean = {
	    val possibleNameSpace : String = if (name != null) name.split('.').head.toLowerCase else null
		val hasNameSpcPrefix : Boolean = if (possibleNameSpace != null) {
			(ctx.namespaceSearchPath.filter( nmspcCandidate => (nmspcCandidate.toLowerCase == possibleNameSpace)).size > 0)
		} else {
			false
		}
		hasNameSpcPrefix
	}
	
	/**
	 *  Find the derived concept's model with the supplied name key (from some FieldRef field). Check the prefix nodes in the 
	 *  presumably '.' delimited name against the known models in the metadata.  If a name with four or more '.' delimited nodes is 
	 *  supplied, the first three are treated as namespace,	modelname and version values. If the name has 3 nodes, treat first
	 *  two names as namespace and model name.  The version is assumed to be the default (latest version).  Two node names 
	 *  are not yet supported (i.e., try the namespaces in namespace search list as prefixes).
	 *  
	 *  The return value is a tuple quartet that contains the ModelDef that computed the derivation, the model name key, the concept name, and optionally
	 *  the subfield names that are to be used to extract some aspect of the concept (when it is a message or container).
	 *  
	 *  Note that the modelName key may have 3, 2, or 1 nodes depending on if the an optional namespace, model name, and optional version
	 *  number has been given.  When no namespace is given, the latest version of the model is assumed (version = -1).  If no namespace is given
	 *  then the first matching namespace in the namespacesearch list is used (<b>NOTE: Not supported yet, but will be at some point. For the 
	 *  time being, only the three node nmspc.name.ver and two node nmspc.name versions of keys are supported.</b>).
	 *  
	 *  Note that the concept name can have multiple nodes as well. There is the conceptName and then, if present, the subfields of the concept when it is 
	 *  a container or message.  
	 *  
	 *  For example, if a derived field in the model holds the most recent event message of an array of events (perhaps the last one),  then some subfield 
	 *  in the event element could be dereferenced with a dot delimited string.  For example,
	 *  
	 *  <code>
	 *  	    <DerivedField name="currentmsg" dataType="SomeMessageType">
     *				<Apply function="Last">
     *  				<FieldRef field="msg.message"/>
     *				</Apply>
     *			</DerivedField>
	 *  </code>
	 *  
	 *  declared in one model called 'consultco.demog1' that had a field called 'zipcode' in it might be accessed from another model with the following derived 
	 *  concept access in a FieldRef:
	 *  
	 *  <code>
     *			<Apply function="LivesOnTheCoast">
     *  			<FieldRef field="consultco.demog1.currentmsg.zipcode"/>
     *			</Apply>
	 *  </code>
	 *  
	 *  In this example, the model container key would be 'consultco.demog1' and the concept name would be 'currentmsg.zipcode'.  Actually the derived concept 
	 *  name <b>is</b> really just 'currentmsg', however, once the concept is retrieved, it's content can be examined with the '.zipcode' dereference.
	 * 
	 *  @param name a String that represents some derived field that presumably has one of the known model names in the 
	 *  	metadata as a prefix. That is, this derived field was produced by some other model
	 *  @return the quartet (ModelDef instance with this key else null, the model container key, derived concept name, remaining subfield nodes)  
	 *   
	 */
	def GetDerivedConceptModel(name : String) : (ModelDef, String, String, String) = {		

    	/** GetDerivedConceptModel helper function , makeKey */
    	def makeKey(names : Array[String], nodesToInclude : Int) : String = {
			val buffer : StringBuilder = new StringBuilder
			for (i <- 0 until nodesToInclude) {
				buffer.append(s"${names(i)}")
				if (i < (nodesToInclude - 1)) {
				    buffer.append('.')
				}	    
			}
			val key : String = buffer.toString
			key
    	}
    	
    	/** Search is from most specific node key (nmspc.nm.ver) to the least specific (nm only).  First match used. */
	    val nodes : Array[String] = if (name != null) name.split('.') else null
	    val (model, modelContainerKey, conceptKey, subFieldKeys) : (ModelDef, String, String, String) = if (nodes != null) {
	    	if (nodes.size > 3) {
	    		val key : String = makeKey(nodes, 3)
	    		val m : ModelDef = GetDerivedConceptModel(key,3)
	    		if (m != null) {
	    			val (containerKeys, concptKeys) : (Array[String], Array[String]) = nodes.splitAt(3)
	    			val containerKeyStr : String = containerKeys.mkString(".")
	    			val (concptKeyStr,subFieldStr) : (String,String) = if (concptKeys.size > 1) {
	    				(concptKeys.head, concptKeys.tail.mkString("."))
	    			} else {
	    				(concptKeys.head,null)
	    			}
	    			(m,containerKeyStr, concptKeyStr, subFieldStr)
	    		} else {
	    			GetDerivedConceptModel(key) /** recurse with the reduced key which will cause the two node case to be tried */
	    		}
	    	} else {
	    		if (nodes.size > 2) {
		    		val key : String = makeKey(nodes, 2)
		    		val m : ModelDef = GetDerivedConceptModel(key,2)
		    		if (m != null) {
		    			val (containerKeys, concptKeys) : (Array[String], Array[String]) = nodes.splitAt(2)
		    			val containerKeyStr : String = containerKeys.mkString(".")
		    			val (concptKeyStr,subFieldStr) : (String,String) = if (concptKeys.size > 1) {
		    				(concptKeys.head, concptKeys.tail.mkString("."))
		    			} else {
		    				(concptKeys.head,null)
		    			}
		    			(m,containerKeyStr,concptKeyStr,subFieldStr)
		    		} else {
		    			GetDerivedConceptModel(key)
		    		}
	    		} else {
	    			if (nodes.size > 1) {
			    		val key : String = makeKey(nodes, 1)
			    		val m : ModelDef = GetDerivedConceptModel(key,1)	
		    			val (containerKeys, concptKeys) : (Array[String], Array[String]) = nodes.splitAt(1)
		    			val containerKeyStr : String = containerKeys.mkString(".")
		    			val (concptKeyStr,subFieldStr) : (String,String) = if (concptKeys.size > 1) {
		    				(concptKeys.head, concptKeys.tail.mkString("."))
		    			} else {
		    				(concptKeys.head,null)
		    			}
		    			(m, containerKeyStr, concptKeyStr, subFieldStr)
	    			} else {
	    				(null,null,null,null)
	    			}
	    		} 
	    	}
	    } else {
	      	(null,null,null,null)
	    }
  
		(model, modelContainerKey, conceptKey, subFieldKeys)

	}
	
	/**
	 *  Find the derived concept's model with the supplied name key (from some FieldRef field). This version of 
	 *  GetDerivedConceptModel serves the public one.  See (@see GetDerivedConceptModel(String) for semantics.
	 * 
	 *  @param name a String that represents some derived field that presumably has one of the known model names in the 
	 *  	metadata as a prefix. That is, this derived field was produced by some other model
	 *  @param numNameNodes is used to search appropriate cache of model names based upon this value.  @see activeModels, 
	 *  	@see activeCurrentVerModels, and @see unqualifiedModelNames for details.
	 *  @return the ModelDef instance with this key else null
	 *  
	 *  FIXME: NOTE: Currently only namespace.modelname.ver and namespace.modelname derived field specifiers are supported.
	 *  Unqualifed modelnames will be added when a more general namespace search path implementation has been 
	 *  implemented that can match arbitrary namespace prefixes with the unqualified model name.
	 *   
	 */
	private def GetDerivedConceptModel(name : String, numNameNodes : Int) : ModelDef = {		
	    val nameNodes : Array[String] = if (name != null) name.split('.') else null
		val (conceptKey,keyNodeCnt) : (String,Int) = if (nameNodes != null && nameNodes.size == 3) {
			(nameNodes(0).toLowerCase() + "." + nameNodes(1).toLowerCase() + "." + nameNodes(2).toLowerCase(), 3)
		} else {
			if (nameNodes != null && nameNodes.size == 2) {
				(nameNodes(0).toLowerCase() + "." + nameNodes(1).toLowerCase(), 2)
			} else {
				if (nameNodes != null && nameNodes.size == 1) {
					(nameNodes(0).toLowerCase(),1)
				} else {
					(null,0)
				}
			}
		}
		
		val model : ModelDef = 	keyNodeCnt match {
			case 3 => {
				if (ctx.activeModels.contains(conceptKey)) ctx.activeModels(conceptKey) else null
			}
			case 2 => {
				if (ctx.activeCurrentVerModels.contains(conceptKey)) ctx.activeCurrentVerModels(conceptKey) else null
			}
			case 1 => {
				val modelExists : String = if (ctx.unqualifiedModelNames.contains(conceptKey)) "" else "not"
				if (modelExists.size == 0) {
					PmmlError.logError(ctx, s"$conceptKey does in fact refer to a model, but derived concept names must be namespace qualified in this release. ")
					logger.error(s"You may wish to suffix it with '.version' of interest to be precise about which version of the model should be used to access the needed value.")
				}
				null
			}
			case _ => {
				PmmlError.logError(ctx, s"This is an illegal derived concept names must be namespace qualified in this release. The FieldRef must have the form:")
				logger.error(s"    namespace.modelname[.version].conceptname where the version may be omitted (current version = default)")
				null
			}
		}

		model
	}
	
	/** 
	 *  Answer if this is a derived concept.  The name must be of the form modelnamespace.modelname[.version].conceptname
	 *  
	 *  @param name a String that represents some derived field that presumably has one of the models in the system as a prefix.
	 *  	That is, this derived field was produced by some other model
	 *  @return true if this is a derived concept else false if not found
	 */
	def IsDerivedConcept(name : String) : Boolean = {
	    val (model, modelContainerKey, conceptKey, subfldKeys) : (ModelDef, String, String, String) = GetDerivedConceptModel(name)
	    (model != null)
	}
	
	/** 
	 *  Answer the model name from this presumably derived concept field name. If version number requested, include it in returned name.
	 *  
	 *  @param name a String that represents some derived field that presumably has one of the models in the system as a prefix.
	 *  	That is, this derived field was produced by some other model
	 *  @param withVersionNum a boolean that when true will suffix the modelnamespace.modelname with .versionnumber
	 *  @return string representation of the model or models that matched this name.  It is possible that multiple models can be returned
	 *  	each with a different version number.  In this case, a comma delimited list is returned.
	 */
	def ModelNameOfDerivedConcept(name : String, withVersionNumber : Boolean = false) : String = {
	  
	    val (model, modelContainerKey, conceptKey, subfldKeys) : (ModelDef, String, String, String) = GetDerivedConceptModel(name)
	    val modelName : String = if (model != null) {
	    	if (withVersionNumber) {
	    		model.FullNameWithVer
	    	} else {
	    		model.FullName
	    	}
	    } else {
	    	"Model.Not.Found"
	    }
		modelName
	}
	
}


