package com.ligadata.Compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import com.ligadata.Pmml.Runtime._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._


class PmmlExecNode (val qName : String) extends LogTrait  { 
	var children : Queue[PmmlExecNode] = new Queue[PmmlExecNode]()
	
	def addChild(child : PmmlExecNode) = {
		children += child
	}
	def replaceChildren(newChildren : Queue[PmmlExecNode]) { children = newChildren }
	
	def Children : Queue[PmmlExecNode] = children
	
	def ElementValue : String = qName
	
	def asString(ctx : PmmlContext) : String = "PMML"
	  
	override def toString : String = s"Node $qName"
	
	def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order) : String = 
	{
		asString(ctx)
	}
	
	/** general access to PmmlExecNode trees .. our node visitor */
	def Visit(visitor : PmmlExecVisitor) {
		visitor.Visit(this)
		children.foreach((child) => {
			child.Visit(visitor)
	  	})
	}

}

class xConstant(val dataType : String, val value : DataValue) extends PmmlExecNode("Constant") {
	def Value : DataValue = value
	
	override def asString(ctx : PmmlContext) : String = {
	  	val strRep : String = value.toString
	  	dataType match {
	  		case "string" | "date" | "time" | "dateTime" => s"${'"'}$strRep${'"'}"
	  		case "ident"  => {
	  			val constStrExpr : String = if (strRep == ctx.applyElementName) {
	  			   s"${ctx.applyElementName}"  /** When the value is _each, the item is being passed as a whole ... common with collection args ... */
	  			} else {
	  				val strRep1 = strRep.toLowerCase
					/** get _each's type from the containerStack to decide whether a mapped or fixed style of field expression is needed */
					val fcnTypeInfo : FcnTypeInfo = if (ctx.fcnTypeInfoStack.nonEmpty) ctx.fcnTypeInfoStack.top else null
					val reasonable : Boolean = (fcnTypeInfo != null && fcnTypeInfo.containerTypeDef != null) // sanity 
					if (! reasonable) {
						if (fcnTypeInfo != null) {
							val fullnm : String = if (fcnTypeInfo.fcnDef.FullName != null) fcnTypeInfo.fcnDef.FullName else "Unknown function"
							  
							PmmlError.logError(ctx, s"iterable function $fullnm does not have a containerType for its Iterable (arg1)")
						} else {
							PmmlError.logError(ctx, "fcnTypeInfo is null for a function with 'ident' names (iterable function)")
						}
						"BadIdent"
					} else {
						if (fcnTypeInfo.containerTypeDef != null && fcnTypeInfo.containerTypeDef.IsFixed) {
							s"${ctx.applyElementName}.$strRep1"  
						} else {
							/** Only add field cast for the MappedMsgTypDef fields at this point since the 'get' fcn returns an Any */
							/** NOTE: the fcnTypeInfo WILL have items in it as the 'ident' type only refers to a (first) sibling container */
							
							val mappedContainer : MappedMsgTypeDef = if (fcnTypeInfo.containerTypeDef.isInstanceOf[MappedMsgTypeDef]) 
									fcnTypeInfo.containerTypeDef.asInstanceOf[MappedMsgTypeDef] else null
							if (mappedContainer != null) {
								val attrType : BaseAttributeDef = mappedContainer.attributeFor(strRep1)
								val attrTypeStr : String = if (attrType != null) attrType.typeString else null
								if (attrTypeStr != null) {
									s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'}).asInstanceOf[$attrTypeStr]"
								} else {
									PmmlError.logError(ctx, s"Field $strRep1 is NOT a field name in the container with type ${mappedContainer.typeString}")
									s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'})" /** this will cause scala compile error (or worse) */
								}
							} else {
								val elementTypes : Array[BaseTypeDef] = fcnTypeInfo.collectionElementTypes
													
								/** NOTE: Only Iterable types with single element type are currently supported. */
								if (elementTypes.size == 1) {
									val baseElemType : BaseTypeDef = elementTypes.last
									val mappedContainerElemType : MappedMsgTypeDef = if (baseElemType.isInstanceOf[MappedMsgTypeDef]) 
										baseElemType.asInstanceOf[MappedMsgTypeDef] else null
									if (mappedContainerElemType != null) {
										val attrType : BaseAttributeDef = mappedContainerElemType.attributeFor(strRep1)
										val attrTypeStr : String = if (attrType != null) attrType.typeString else null
										if (attrTypeStr != null) {
											s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'}).asInstanceOf[$attrTypeStr]"
										} else {
											PmmlError.logError(ctx, s"Field $strRep1 is NOT a field name in the container with type ${mappedContainer.typeString}")
											s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'})" /** this will cause scala compile error */
										}
									} else { /** one more time... see if the element is also an array (i.e., array of array of some (mapped or fixed) struct */
										if (baseElemType.isInstanceOf[StructTypeDef]) {
											s"${ctx.applyElementName}.$strRep1"
										} else {
											val nestedElems : Array[BaseTypeDef] = if (baseElemType.isInstanceOf[ContainerTypeDef]) {
													baseElemType.asInstanceOf[ContainerTypeDef].ElementTypes
												} else {
													null
												}
											if (nestedElems != null && nestedElems.size == 1) {
												val nestedBaseElemType : BaseTypeDef = nestedElems.last
												val nestedMappedContainerElemType : MappedMsgTypeDef = if (nestedBaseElemType.isInstanceOf[MappedMsgTypeDef]) 
													nestedBaseElemType.asInstanceOf[MappedMsgTypeDef] else null
												if (nestedMappedContainerElemType != null) {
													val attrType : BaseAttributeDef = nestedMappedContainerElemType.attributeFor(strRep1)
													val attrTypeStr : String = if (attrType != null) attrType.typeString else null
													if (attrTypeStr != null) {
														s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'}).asInstanceOf[$attrTypeStr]"
													} else {
														PmmlError.logError(ctx, s"Field $strRep1 is NOT a field name in the container with type ${mappedContainer.typeString}")
														s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'})" /** this will cause scala compile error */
													}
												} else {
													s"${ctx.applyElementName}.$strRep1"
												}										
											} else {	
												if (nestedElems != null) {
													PmmlError.logWarning(ctx, s"Type ${baseElemType.typeString} has ${nestedElems.size} elements... we are only supporting collections with single 'mapped' type elements at the moment")
												}
												s"${ctx.applyElementName}.$strRep1"
											}
										}
									}
								} else {
									PmmlError.logWarning(ctx, s"Type ${fcnTypeInfo.containerTypeDef.typeString} has ${elementTypes.size} elements... we are only supporting collections with single 'mapped' type elements at the moment")
									s"${ctx.applyElementName}.get(${'"'}$strRep1${'"'})"
								}				  
							}
						}
					}
	  			}
	  			constStrExpr
	  		}
	  		case "typename"  => { 
	  		  /** 
	  		   *  The constant value is either the name of a datafield or a derived field.  Retrieve the type info for
	  		   *  the dataType of this field and return its typeString
	  		   */ 
	  		  val expandCompoundFieldTypes : Boolean = false
	  		  val fldTypeInfo : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(strRep, expandCompoundFieldTypes) 
	  		  val (typestr, isContainerWMbrs, typedef) : (String,Boolean,BaseTypeDef) = fldTypeInfo.last
	  		  if (typestr != null) typestr else "Any"
	  		}
	  		case "mbrTypename"  => { 
	  		  /** 
	  		   *  The constant value is either the name of a datafield or a derived field that is either an Array
	  		   *  or ArrayBuffer (only collection types currently supported).  Retrieve the type info for member 
	  		   *  type of this field's Array or ArrayBuffer type and return its typeString
	  		   */ 
	  		  val expandCompoundFieldTypes : Boolean = false
	  		  val fldTypeInfo : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(strRep, expandCompoundFieldTypes) 
	  		  val (typestr, isContainerWMbrs, typedef) : (String,Boolean,BaseTypeDef) = fldTypeInfo.last
	  		  if (typedef != null && (typedef.isInstanceOf[ArrayTypeDef] || typedef.isInstanceOf[ArrayBufTypeDef])) {
	  			  val typeStr : String = typedef match {
	  			    case ar : ArrayTypeDef => typedef.asInstanceOf[ArrayTypeDef].elemDef.typeString
	  			    case arb : ArrayBufTypeDef => typedef.asInstanceOf[ArrayTypeDef].elemDef.typeString
	  			    case _ => "Any"
	  			  }
	  			  typeStr 
	  		  } else "Any"
	  		}

	  		case _ => strRep
	  	}
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String = {
		asString(ctx)
	}
}

class xHeader(val copyright : String, val description : String) extends PmmlExecNode("Header") {

	var applicationName : String = "Some App"
	var version : String = "Some Version"
	def ApplicationNameVersion(appName : String, ver : String) {
		applicationName = appName
		version = ver
	}
	override def asString(ctx : PmmlContext) : String =  {
			val header1 = s"/** Copyright : $copyright Application : $applicationName Version : $version\n"
			val header2 = s"    Description : $description\n"
			val header = s"$header1\n$header2 */"
			header
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		asString(ctx)
	}

}


class dateDaysSince0(timesecs : Long)
class dateDaysSince1960(timesecs : Long)
class dateDaysSince1970(timesecs : Long)
class dateDaysSince1980(timesecs : Long)
class dateTimeSecondsSince0(timesecs : Long)
class dateTimeSecondsSince1960(timesecs : Long)
class dateTimeSecondsSince1970(timesecs : Long)
class dateTimeSecondsSince1980(timesecs : Long)
class timeSeconds(timesecs : Long)
class xDataField(val name : String
				, val displayName : String
				, val optype : String
				, val dataType : String
				, val taxonomy : String
				, val isCyclic : String) extends PmmlExecNode("DataField") {
	var values : ArrayBuffer[(String,String)] = ArrayBuffer[(String,String)]() /** (value, property) */
	var leftMargin : String = ""
	var rightMargin : String =""
	var closure : String = ""
	
	def addValuePropertyPair(value : String, proprty : String) {
		values += Tuple2(value, proprty)
	}
	def continuousConstraints(left : String, right : String, closr : String) {
		leftMargin = left
		rightMargin = right;
		closure = closr
	}

	override def asString(ctx : PmmlContext) : String = 
	{
		val typ = PmmlTypes.scalaDataType(dataType)
		val variable = s"var $name : $typ "
		variable
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val typeStr : String = PmmlTypes.scalaDataType(dataType)
		
		val fieldDecl : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				generate match {
					case CodeFragment.VARDECL => {
						s"val $name : $typeStr "
					}
					case CodeFragment.VALDECL => {
						s"var $name : $typeStr "						
					}
					case CodeFragment.FUNCCALL => {
						/** generate the code to fetch the value */ 
						
						/** FIXME:  HACK ALERT!!! When the field refers to "beneficiary... " we really don't want to retrieve a value 
						 *  from the dictionaries.  Instead this field reference refers to a the extracted data found in ctx.Beneficiary... 
						 *  With this in mind, we don't do the lookup for the value.  Instead, we simply return the beneficiary reference as a string.
						 */
		
						if (name.startsWith("beneficiary")) {
							name
						} else {
							s"ctx.valueFor(${'"'}$name${'"'})"			  
						}
					} 
					case _ => { 
						PmmlError.logError(ctx, "DataField node - unsupported CodeFragment.Kind") 
						""
					}
				}
			}
		}
		fieldDecl
	}

}


class xDataDictionary extends PmmlExecNode("DataDictionary") {
	var map = new HashMap[String,xDataField]()
	
	def DataDictionary = { map }
	
	def add(node : xDataField) {
		map += (node.name -> node)
	}

	override def asString(ctx : PmmlContext) : String = 
	{
		val dictBuffer : StringBuilder = new StringBuilder()
		dictBuffer.append("dataDict : Map (\n")
		
		val beginchars : String = "\n\t\t"
		map.foreach {
		  	case ( _,dfld) => { 
			  		val dName = dfld.name
			  		val typ = PmmlTypes.scalaDataType(dfld.dataType)

		    		dictBuffer.append(s"$beginchars${'"'}$dName${'"'} => ${'"'}$typ${'"'}")
		  		}
			}
		dictBuffer.append(beginchars)
		dictBuffer.append(")")
		dictBuffer.toString
  	}

	def asCode : String = {
		val dictBuffer : StringBuilder = new StringBuilder()
		//dictBuffer.append("dataDict : Map[String, DataField]()\n")  .. pre-declared as empty in the context 
		
		var serNo : Int = 0
		map.foreach {
		  	case ( _,dfld) => { 
		  		val dName = dfld.name
				val dNameFixer = "[-.]+".r
				val className : String = dNameFixer.replaceAllIn(dName,"_")

				val typ = PmmlTypes.scalaDataType(dfld.dataType)
		  		val lm : String = dfld.leftMargin
		  		val rm : String = dfld.rightMargin
		  		val cls : String = dfld.closure
		  		serNo += 1
		  		val valnm = s"dfoo$serNo"
		  		var valuesStr = NodePrinterHelpers.valuesHlp(dfld.values, valnm, "        ")
		  		dictBuffer.append(s"        var $valnm : ArrayBuffer[(String,String)] = $valuesStr\n")
		  		var dataFieldStr : String = s"        ctx.dDict += (${'"'}$dName${'"'} -> new DataField(${'"'}$dName${'"'}, ${'"'}$typ${'"'}, $valnm, ${'"'}$lm${'"'}, ${'"'}$rm${'"'}, ${'"'}$cls${'"'}))\n"
	    		dictBuffer.append(dataFieldStr)
		  	}
		}
		dictBuffer.toString
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val dictDecl : String = order match {
			case Traversal.PREORDER => asCode
			case _ => {
				PmmlError.logError(ctx, s"TransformationDictionary only supports Traversal.PREORDER")
				""
			}
		}
		dictDecl
	}
}	 

class xDerivedField(val name : String
					, val displayName : String
					, val optype : String
					, val dataType : String) extends PmmlExecNode("DerivedField") {
	var values : ArrayBuffer[(String,String)] = ArrayBuffer[(String,String)]() /** (value, property) */
	var leftMargin : String = _
	var rightMargin : String = _
	var closure : String = ""

	var categorizedValues : ArrayBuffer[xConstant] = ArrayBuffer[xConstant]()
	def CategorizedValue(idx : Int) : xConstant = categorizedValues.apply(idx)
	def CategorizedValues : ArrayBuffer[xConstant] = categorizedValues
	def addCategorizedValue(aConst : xConstant)  : ArrayBuffer[xConstant] = { categorizedValues += aConst }
	
	def addValuePropertyPair(value : String, proprty : String) {
		values += Tuple2(value, proprty)
	}
	def continuousConstraints(left : String, right : String, closr : String) {
		leftMargin = left
		rightMargin = right;
		closure = closr
	}
	
	def Name(ctx : PmmlContext) : String = { 
	  name
	}
	
	override def toString : String = s"DerivedField '$name'"

	override def asString(ctx : PmmlContext) : String = 
	{
		val typ = PmmlTypes.scalaDataType(dataType)
		val fcnNm = name + "_Fcn"
		val variable = s"var $name : $typ = $fcnNm"
		variable
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, kind : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		ctx.elementStack.push(this) /** track the element as it is processed */
		
		val typeStr : String = PmmlTypes.scalaDataType(dataType)
		
		val fieldDecl : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				kind match {
					case CodeFragment.DERIVEDCLASS => {
						ctx.fcnTypeInfoStack.clear /** if there is cruft, kill it now as we prep for dive into derived field and its function hierarchy*/
						NodePrinterHelpers.derivedFieldFcnHelper(this, ctx, generator, kind, order)
					}
					case CodeFragment.FUNCCALL => {
						/** generate the code to fetch the value  
						
						if (name.startsWith("beneficiary")) {
							name
						} else {
							s"ctx.valueFor(${'"'}$name${'"'})"			  
						}*/
					  ""
					} 
					case _ => { 
						PmmlError.logError(ctx, "DerivedField node - unsupported CodeFragment.Kind") 
						""
					}
				}
			}
		}
		ctx.elementStack.pop 	/** done discard the current element */
		
		fieldDecl
	}
}

class xTransformationDictionary() extends PmmlExecNode("TransformationDictionary") {
	var map = new HashMap[String,xDerivedField]()
	
	def add(node : xDerivedField) {
		map += (node.name -> node)
	}
	override def asString(ctx : PmmlContext) : String = 
	{
		val dictBuffer : StringBuilder = new StringBuilder()
		dictBuffer.append("var xDict : Map (\n")

		val beginchars : String = "\n\t\t"
		map.foreach {
		  	case ( _,dfld) => { 
			  		val dName = dfld.name
			  		val typ = PmmlTypes.scalaDataType(dfld.dataType)
		
		    		dictBuffer.append(s"$beginchars${'"'}$dName${'"'} => ${'"'}$typ${'"'}")
		  		}
			}
		dictBuffer.append(beginchars)
		dictBuffer.append(")")
		dictBuffer.toString
  	}

	def asCode : String = {
		val dictBuffer : StringBuilder = new StringBuilder()
		//dictBuffer.append("dataDict : Map[String, DataField]()\n").. pre-declared as empty in the context 
		
		var serNo : Int = 0
		map.foreach {
		  	case ( _,dfld) => { 
	  			/** pmmlElementVistitorMap += ("Constant" -> PmmlNode.mkPmmlConstant) */
		  		val dName = dfld.name
		  		
				val classname : String = s"Derive_$dName"
				val classNameFixer = "[-.]+".r
				val className : String = classNameFixer.replaceAllIn(classname,"_")

		  		val typ = PmmlTypes.scalaDataType(dfld.dataType)
		  		val lm : String = dfld.leftMargin
		  		val rm : String = dfld.rightMargin
		  		val cls : String = dfld.closure
		  		serNo += 1
		  		val serNoStr = serNo.toString

		  		val valnm = s"xbar$serNoStr"
		  		var valuesStr = NodePrinterHelpers.valuesHlp(dfld.values, valnm, "        ")
		  		dictBuffer.append(s"        var $valnm : ArrayBuffer[(String,String)] = $valuesStr\n")
		  		var derivedFieldStr : String = s"        ctx.xDict += (${'"'}$dName${'"'} -> new $className(${'"'}$dName${'"'}, ${'"'}$typ${'"'}, $valnm, ${'"'}$lm${'"'}, ${'"'}$rm${'"'}, ${'"'}$cls${'"'}))\n"
	    		dictBuffer.append(derivedFieldStr)
		  	}
		}
		dictBuffer.toString
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, kind : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val codeStr : String = order match {
			case Traversal.PREORDER => {
				kind match {
				  case CodeFragment.VALDECL => asCode
				  case CodeFragment.DERIVEDCLASS => {
					  	val clsBuffer : StringBuilder = new StringBuilder()
					  	
						Children.foreach((child) => {
							generator.generateCode1(Some(child), clsBuffer, generator, kind)
				  		})
					  	
				    	clsBuffer.toString
				  }
				  case _ => { 
					  PmmlError.logError(ctx, s"fragment kind $kind not supported by TransformationDictionary")
				      ""
				  }
				}
			}
			case _ => {
				PmmlError.logError(ctx, s"TransformationDictionary only supports Traversal.PREORDER")
				""
			}
		}
		codeStr
	}
}	 

class xDefineFunction(val name : String
					, val optype : String
					, val dataType : String)  extends PmmlExecNode("DefineFunction") {	
	override def asString(ctx : PmmlContext) : String = 
	{
		val applyv = s"name = $name, optype = $optype, dataType = $dataType"
		applyv
	}
	
	def NumberArguments : Int = {
		Children.length
	}
	
	def ArgumentTypes : Array[String] = {
		    
		val types  = Children.map(child => {
				child match {
				  	case parm : xParameterField => PmmlTypes.scalaDataType(parm.dataType)
				  	case _ => "String"
				 }
			}).toArray
		types
			
	}
	
	def ArgumentDecls : Array[String] = {
		/** FIXME build scala argument list here instead of this place holder that returns the ArgumentTypes */
		ArgumentTypes
	}
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
	  
	  /**
	   	
	   	
	   	  The function decl gets stored in the function dict of the context.  
	   	  It has the function and parameter fields... usable for type checking 
	   	  or for c++ generation situations 
	   	
	   	  For now we don't print anything for these <<<<<<<<<<<<<<<<<<<<
	   	
	   	
	   */
		""
	}

}

class xParameterField(val name : String
					, val optype : String
					, val dataType : String)  extends PmmlExecNode("ParameterField") {	

	override def asString(ctx : PmmlContext) : String = 
	{
		val applyv = s"name = $name, optype = $optype, dataType = $dataType"
		applyv
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
	  /**
	   	
	   	
	   	  The function decl gets stored in the function dict of the context.  
	   	  It has the function  and parameter fields... usable for type checking 
	   	  or for c++ generation situations 
	   	
	   	  For now we don't print anything for these <<<<<<<<<<<<<<<<<<<<
	   	
	   	
	   **/
	  
		""
	}


}

class xApply(val function : String, val mapMissingTo : String, val invalidValueTreatment : String = "returnInvalid") extends PmmlExecNode("Apply") {
  
	var typeInfo : FcnTypeInfo = null
	def SetTypeInfo(typInfo : FcnTypeInfo) = { typeInfo = typInfo }
	def GetTypeInfo : FcnTypeInfo = { typeInfo }
	
	var ifActionElements : ArrayBuffer[PmmlExecNode] = ArrayBuffer[PmmlExecNode]()
	def IfActionElement(idx : Int) : PmmlExecNode = ifActionElements.apply(idx)
	def IfActionElements : ArrayBuffer[PmmlExecNode] = ifActionElements
	def addIfAction(anAction : PmmlExecNode)  : ArrayBuffer[PmmlExecNode] = { ifActionElements += anAction; ifActionElements }
	
	override def asString(ctx : PmmlContext) : String = 
	{
		val applyv = s"function = $function"
		applyv
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val fcn : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {

				/** for iterables ... push the function type info state on stack for use by xConstant printer when handling 'ident' types */
				if (typeInfo != null && typeInfo.fcnTypeInfoType != FcnTypeInfoType.SIMPLE_FCN) {
					ctx.fcnTypeInfoStack.push(typeInfo)
				}
				
				val fcnStr : String = NodePrinterHelpers.applyHelper(this, ctx, generator, generate, order)

				if (typeInfo != null && typeInfo.fcnTypeInfoType != FcnTypeInfoType.SIMPLE_FCN && ctx.fcnTypeInfoStack.nonEmpty) {
					val fcnTypeInfo : FcnTypeInfo = ctx.fcnTypeInfoStack.top
					logger.trace(s"finished printing apply function $function... popping FcnTypeInfo : \n${fcnTypeInfo.toString}")
					ctx.fcnTypeInfoStack.pop
				} 
				
				fcnStr
			}
		}
		fcn
	}
	
	override def toString : String = s"Apply function '$function'"


}

class xFieldRef(val field : String, val mapMissingTo : String ) extends PmmlExecNode("FieldRef") {
  
	override def asString(ctx : PmmlContext) : String = 
	{
		val fr = s"FieldRef(field = $field)"
		fr
	}

	override def codeGenerator(ctx : PmmlContext
							, generator : PmmlModelGenerator
							, generate : CodeFragment.Kind
							, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val fldRef : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {

				PmmlExecNode.prepareFieldReference(ctx, field)
			}
		}
		fldRef
	}
}

class xExtension(val extender : String, val name : String , val value : String ) extends PmmlExecNode("Extension") {

	override def asString(ctx : PmmlContext) : String = 
	{
		val ext = s"Extension (extender = $extender, name = $name, value = $value)"
		ext
	}
	
	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val ext : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}Extension $name${'"'})"
			}
		}
		ext
	}
  
}     

/** 
 *  The xMapValues structure will be used to represent queries result sets and tables.  Both Maps and 
 *  Vectors of Tuples are supported.  The expectation is that the field will contain the necessary
 *  field names to name the content in the 'map'.  For the standard use case (a standard key/value map), 
 *  the fieldColumnMap will have the "database" field name as the key (e.g., beneficiary.pufId or 
 *  a schema qualified one - cms.beneficiary.pufId) and the value will be the "internal" column name 
 *  to use to refer to the data in the loaded table (whether it is from an external database or file, 
 *  or an in-line table declaration. 
 *  
 *  In terms of the MapValue syntax tree nodes, there will be these kinds:
 *      o xMapValuesMapExternal		- for TableLocator based Map requests
 *  	o xMapValuesMapInline		- for InlineTable based Map requests
 *   	o xMapValuesArrayExternal	- for TableLocator based Array of tuples
 *    	o xMapValuesArrayInline		- for InlineTable based Array of tuples
 *  
 *   The TableLocator is used for external table content
 *  
 *  	<TableLocator>
 *         <Extension> name="connection string" value="blah blah"/>
 *         <Extension> name="table" value="humana.beneficiary"/>
 *         <Extension> extender="inner join on pufId" name="tablenames" value="beneficiaries,outPatient,inPatient"
 *         <Extension> name="filter" value="some filter"
 *         <Extension> name="fieldname" value="pufId"
 *         <Extension> name="fieldname" value="inSummary"
 *         <Extension> extender="Aggregate" name="sum(inCost)" value="inPatientCosts"
 *         <Extension> extender="Aggregate" name="sum(outCost)" value="outPatientCosts"
 *         .
 *         .
 *         .
 *         <Aggregate field="outPatientCosts" function="sum" groupField="pufId"/>
 *         <Aggregate field="outPatientCosts" function="sum" groupField="pufId"/>
 *      </TableLocator>
 *      
 *      Since the Extension attributes are not checked by the syntax checkers, I think this is ok.  
 *      Obviously this needs more thought to properly and succinctly represent the data base query.
 *      We may want to define our own type here (e.g, XligaQuery) rather than general scheme.  Also
 *      it seems like just giving the full query as the value of one of the TableLocator's Extensions
 *      might be good.  If so, then additional queries that gives the metadata for the columns and functions
 *      used in it might also be good:
 *      
 *  	<TableLocator>
 *         <Extension> name="connection string" value="blah blah"/>
 *         <Extension> extender = "data" name="query" value="select... blah blah ... order by 1,2"/>
 *         <Extension> extender="metadata" name="columns" value="select attname, atttyp, isarray from mpg_attribute where..."/>
 *         <Extension> extender="metadata" name="functions" value="select ... from pg_proc ..."/>
 *         <Extension> extender="metadata" name="tables" value="select ... from pg_class ..."/>
 *      </TableLocator>
 *      
 *      
 *      File loads could be represented like this:
 *      
 *  	<TableLocator>
 *         <Extension> name="tableType" value="<{Map | Vector}>"/>
 *         <Extension> extender="metadata" name="metafile" value="<metadata-description-file-path>"/>
 *         <Extension> extender="data" name="datafile" value="<data-file-path>"/> <!-- metadata in line above describes this file -->
 *      </TableLocator>
 *      
 *      Metadata could be a serialized dictionary....
 *      
 *      Inline tables could be like this:
 *      
 *      <InlineTable>
 *          <Extension> extender="metadata" name="file" value="<metadata-description-file-path>"/> <!-- describes row tuples -->
 *          <Extension> name="tableType" value="<{Map | Vector}>"/>
 *          <row><key>m</key><value>male</value>
 *          .
 *          .
 *          .
 *      </InlineTable>
 *              
 */
class xMapValuesMap(val mapMissingTo : String
					, val defaultValue : String
					, val outputColumn : String
					, val dataType : String
					, val treatAsKeyValueMap : String
					, val dataSourceIsInline : String) extends PmmlExecNode("MapValuesMap") {
	var fieldColumnMap : Map[String,String]  = Map[String,String]()
	var table : Map[String,String] = Map[String,String]()
	var metadata : Map[String, Tuple2[String,Boolean]] = Map[String,Tuple2[String,Boolean]]() /** colnm/(type,isarray) */
	var extensions : Array[xExtension] = Array[xExtension]()
	
	def add(key: String, value : String) {
		table(key) = value
	}
	def addFieldColumnPair ( fieldname : String, column : String) {
		fieldColumnMap(fieldname) = column
	}
	def addMetaData ( column : String, meta : Tuple2[String,Boolean]) {
		metadata(column) = meta
	}
	def addExtensions ( xs : Array[xExtension]) {
		extensions ++= xs
	}
 
	/** general diagnostic function to see what is there */
	override def asString(ctx : PmmlContext) : String = 
	{
		val typ = PmmlTypes.scalaDataType(dataType)
		val ext = s"xMapValuesMap(outputColumn = $outputColumn dataType = $typ"
		ext
	}

	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVM : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVM
	}
}

class xMapValuesMapInline(mapMissingTo : String
					, defaultValue : String
					, outputColumn : String
					, dataType : String
					, val containerStyle : String
					, val dataSource : String) 
			extends xMapValuesMap( mapMissingTo, defaultValue, outputColumn, dataType, containerStyle, dataSource) {

	/** general diagnostic function to see what is there */
	override def asString(ctx : PmmlContext) : String = 
	{
		val typ = PmmlTypes.scalaDataType(dataType)
		val ext = s"xMapValuesMapInline(outputColumn = $outputColumn dataType = $typ"
		ext
	}
	
	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVMI : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVMI
	}

}

class xMapValuesMapExternal(mapMissingTo : String
					, defaultValue : String
					, outputColumn : String
					, dataType : String
					, containerStyle : String
					, dataSource : String)
			extends xMapValuesMap(mapMissingTo, defaultValue, outputColumn, dataType, containerStyle, dataSource) {

	/** FIXME : need containers here to capture the content in the TableLocator and represent the data and metadata queries */
  override def asString(ctx : PmmlContext) : String = 
	  {
		val typ = PmmlTypes.scalaDataType(dataType)
			val ext = s"xMapValuesMapExternal(outputColumn = $outputColumn dataType = $typ"
			ext
	  }

  	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVME : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVME
	}
}

class xMapValuesArray(val mapMissingTo : String
					, val defaultValue : String
					, val outputColumn : String
					, val dataType : String
					, val containerStyle : String
					, val dataSource : String)
			extends PmmlExecNode("MapValuesArray") {
	var fieldColumnMap : Map[String,String]  = Map[String,String]() /** maps external table names to internal */
	var table : ArrayBuffer[String] = ArrayBuffer[String]()
	var metadata : Map[String, Tuple2[String,Boolean]] = Map[String,Tuple2[String,Boolean]]() /** colnm/(type,isarray) */
	var extensions : Array[xExtension] = Array[xExtension]()
	
	def add(key: String, value : String) {
		table += value
	}
	def addFieldColumnPair ( fieldname : String, column : String) {
		fieldColumnMap(fieldname) = column
	}
	def addMetaData ( column : String, meta : Tuple2[String,Boolean]) {
		metadata(column) = meta
	}
	def addExtensions ( xs : Array[xExtension]) {
		extensions ++= xs
	}
	override def asString(ctx : PmmlContext) : String = {
		val typ = PmmlTypes.scalaDataType(dataType)
		val ext = s"xMapValuesArray(outputColumn = $outputColumn dataType = $typ"
		ext
	}

  	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVA : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVA
	}
}

class xMapValuesArrayExternal(mapMissingTo : String
					, defaultValue : String
					, outputColumn : String
					, dataType : String
					, containerStyle : String
					, dataSource : String)
			extends xMapValuesArray(mapMissingTo, defaultValue, outputColumn, dataType, containerStyle, dataSource) {

	/** FIXME : need containers here to capture the content in the TableLocator and represent the data and metadata queries */
	override def asString(ctx : PmmlContext) : String = {
		val typ = PmmlTypes.scalaDataType(dataType)
		val ext = s"xMapValuesArrayExternal(outputColumn = $outputColumn dataType = $typ"
		ext
	}

  	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVAE : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVAE
	}
}

class xMapValuesArrayInline(mapMissingTo : String
					, defaultValue : String
					, outputColumn : String
					, dataType : String
					, containerStyle : String
					, dataSource : String)
			extends xMapValuesArray(mapMissingTo, defaultValue, outputColumn, dataType, containerStyle, dataSource) {

	/** FIXME : need containers here to capture the content in the TableLocator and represent the data and metadata queries */
	override def asString(ctx : PmmlContext) : String = {
		val typ = PmmlTypes.scalaDataType(dataType)
		val ext = s"xMapValuesArrayInline(outputColumn = $outputColumn dataType = $typ"
		ext
	}

  	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val mapVAI : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $outputColumn${'"'})"
			}
		}
		mapVAI
	}
}

/**xMapValuesVectorExternal
 
	<xs:element name="TableLocator">
	  <xs:complexType>
	    <xs:sequence>
	      <xs:element ref="Extension" minOccurs="0" maxOccurs="unbounded"/>
	    </xs:sequence>
	  </xs:complexType>
	</xs:element> 
	
    <TableLocator>
      <Extension name="dbname" value="myDB"/>
    </TableLocator>
    
  <xs:element name="Extension">
    <xs:complexType>
      <xs:complexContent mixed="true">
        <xs:restriction base="xs:anyType">
          <xs:sequence>
            <xs:any processContents="skip" minOccurs="0" maxOccurs="unbounded"/>
          </xs:sequence>
          <xs:attribute name="extender" type="xs:string" use="optional"/>
          <xs:attribute name="name" type="xs:string" use="optional"/>
          <xs:attribute name="value" type="xs:string" use="optional"/>
        </xs:restriction>
      </xs:complexContent>
    </xs:complexType>
  </xs:element>
 
*/


class xFieldColumnPair(field : String, column : String) extends PmmlExecNode("FieldColumnPair") {

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xFieldColumnPair(field = $field column = $column)"
		ext
	}

  	/** 
	 *  FIXME: when the MapValues are implemented....
	 */
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val fldColPr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s"ctx.dDict.apply(${'"'}MapValues $field${'"'})"
			}
		}
		fldColPr
	}
}

class xMiningField(val name : String
			    , val usageType : String
			    , val opType : String
			    , var importance : Double
			    , val outliers : String
			    , var lowValue : Double
			    , var highValue : Double
			    , val missingValueReplacement : String
			    , val missingValueTreatment : String
			    , val invalidValueTreatment : String) extends PmmlExecNode("MiningField") {

	def Importance(impertance : Double) { importance = impertance }
	def LowValue(lval : Double) { lowValue = lval }
	def HighValue(hval : Double) { highValue = hval }

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xMiningField(name = $name)"
		ext
	}
 
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val fld : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => { /** arguments for the add mining field function in RuleSetModel */
			    generate match {
			      case CodeFragment.MININGFIELD => {
			    	  s"ruleSet.AddMiningField(${'"'}$name${'"'}, new MiningField(${'"'}$name${'"'},${'"'}$usageType${'"'},${'"'}$opType${'"'},${'"'}$importance${'"'},${'"'}$outliers${'"'},${'"'}$lowValue${'"'},${'"'}$highValue${'"'},${'"'}$missingValueReplacement${'"'},${'"'}$missingValueTreatment${'"'},${'"'}$invalidValueTreatment${'"'}))\n"
			      }
			      case _ => {
			    	  PmmlError.logError(ctx, "MiningField .. Only CodeFragment.MININGFIELD is supported")
			    	  ""
			      }
			    }
			}
		}
		fld
	}
}

class xRuleSelectionMethod(val criterion : String) extends PmmlExecNode("RuleSelectionMethod") {

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val selMeth : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s" new RuleSelectionMethod(${'"'}$criterion${'"'})"
			}
		}
		selMeth
	}
}

class xRuleSetModel(val modelName : String
				, val functionName : String
				, val algorithmName : String
				, val isScorable : String) extends PmmlExecNode("RuleSetModel") {
	var miningSchemaMap : HashMap[String,xMiningField] = HashMap[String,xMiningField]()
	var ruleSet : ArrayBuffer[xSimpleRule] = ArrayBuffer[xSimpleRule]()
	var ruleSetSelectionMethods : ArrayBuffer[xRuleSelectionMethod] = ArrayBuffer[xRuleSelectionMethod]()
	var defaultScore : String = "NONE"
	
	def MiningSchemaMap : HashMap[String,xMiningField] = { miningSchemaMap }
	
	def addMiningField(fieldName : String, fieldDetails: xMiningField) {
		miningSchemaMap(fieldName) = fieldDetails
	}

	def addRule(r : xSimpleRule) {
		ruleSet += r
	}

	def addRuleSetSelectionMethod(m : xRuleSelectionMethod) {
		ruleSetSelectionMethods += m
	}

	def outputFieldNames() : Array[String] = {
		val outputs = miningSchemaMap.filter((m) => m._2.opType == "predicted" || m._2.opType == "supplementary")
		outputs.keys.toArray
	}

	def outputFields() : Array[xMiningField] = {
		val outputs = miningSchemaMap.filter((m) => m._2.opType == "predicted" || m._2.opType == "supplementary")
		outputs.values.toArray
	}
	
	def DefaultScore (s : String) {
		defaultScore = s
	}

	def DefaultScore : String = defaultScore
	

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xRuleSetModel(modelName = $modelName)"
		ext
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, kind : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val rsm : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				kind match {
					case CodeFragment.RULESETCLASS => {
						NodePrinterHelpers.ruleSetModelHelper(this, ctx, generator, kind, order)						
					}
					case CodeFragment.RULECLASS | CodeFragment.MININGFIELD => {  /** continue diving for the RULECLASS and MININGFIELD generators */
						val clsBuffer : StringBuilder = new StringBuilder()
						Children.foreach((child) => {
							generator.generateCode1(Some(child), clsBuffer, generator, kind)
						})
					   	clsBuffer.toString
					}
					case _ => { 
						val kindStr : String = kind.toString
						PmmlError.logError(ctx, s"RuleSetModel node - unsupported CodeFragment.Kind - $kindStr") 
						""
					}
				}
			}
		}
		rsm
	}
}

class xRuleSet(val recordCount : String
				, val nbCorrect : String
				, val defaultScore : String
				, val defaultConfidence : String)  extends PmmlExecNode("RuleSet") {

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xRuleSet(recordCount = $recordCount, nbCorrect = $nbCorrect, defaultScore = $defaultScore, defaultConfidence = $defaultConfidence )"
		ext
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, kind : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val rsm : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				ctx.logger.debug("xRuleSet is not used for code generation... the important information it possesses has been given to its parent xRuleSetModel")
				ctx.logger.debug("xRuleSet children are important, however.  They are available through an instance of this class.")

				val clsBuffer : StringBuilder = new StringBuilder()
				Children.foreach((child) => {
					generator.generateCode1(Some(child), clsBuffer, generator, kind)
				})
			   	clsBuffer.toString
			}
		}
		rsm
	}
}

class xSimpleRule(val id : Option[String]
			    , val score : String
			    , var recordCount : Double
			    , var nbCorrect : Double
			    , var confidence : Double
			    , var weight : Double) extends PmmlExecNode("SimpleRule") {
  
	var scoreDistributions : ArrayBuffer[xScoreDistribution] = ArrayBuffer[xScoreDistribution]()
	
	def Name(ctx : PmmlContext) : String = { 
	  val idstr : String = id match {
	    case Some(id) => "RuleId_" + id + ctx.Counter()
	    case _ => "RuleId_NoId_" + ctx.Counter()
	  }
	  idstr
	}

	def addScoreDistribution(sd : xScoreDistribution) {
		scoreDistributions += sd
	}

	def RecordCount(rCount : Double) {
		recordCount = rCount
	}

	def CorrectCount(cCount : Double) {
		nbCorrect = cCount
	}

	def Confidence(conf : Double) {
		confidence = conf
	}

	def Weight(wt : Double) {
		weight = wt
	}

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xSimpleRule(id = $id)"
		ext
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val simpRule : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				generate match {
					case CodeFragment.RULECLASS => {
						NodePrinterHelpers.simpleRuleHelper(this, ctx, generator, generate, order)
					}
					case CodeFragment.MININGFIELD => {
						/** 
						 *  Ignore this one. The visiting method will pass this down to the MiningSchema and the RuleSet children and their children. 
						 *  It only is meaningful in the MiningSchema path
						 */
						""
					}
					case _ => { 
						val kindStr : String = generate.toString
						PmmlError.logError(ctx, s"SimpleRule node - unsupported CodeFragment.Kind - $kindStr") 
						""
					}
				}
			}
		}
		simpRule
	}
}

class xScoreDistribution(var value : String
			    , var recordCount : Double
			    , var confidence : Double
			    , var probability : Double) extends PmmlExecNode("ScoreDistribution") {

	def RecordCount(rc : Double) { recordCount = rc }
	def Confidence(conf : Double) { confidence = conf }
	def Probability(prob : Double) { probability = prob }
	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xScoreDistribution(value = $value)"
		ext
	}
	
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val scoreDist : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s" new ScoreDistribution(${'"'}$value${'"'}, ${'"'}$recordCount${'"'}, ${'"'}$confidence${'"'}, ${'"'}$value${'"'}, ${'"'}$probability${'"'})"
			}
		}
		scoreDist
	}
}

class xCompoundPredicate(val booleanOperator : String) extends PmmlExecNode("CompoundPredicate") {

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xCompoundPredicate(booleanOperator = $booleanOperator)"
		ext
	}
  
	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
	  	val fcnBuffer : StringBuilder = new StringBuilder()
		val compoundPredStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				val boolOpFcn : String = PmmlTypes.scalaBuiltinNameFcnSelector(booleanOperator)
				val cPred = s"$boolOpFcn("
				fcnBuffer.append(cPred)
				var i : Int = 0

		  		Children.foreach((child) => {
		  			i += 1
			  		generator.generateCode1(Some(child), fcnBuffer, generator, CodeFragment.FUNCCALL)
			  		if (i < Children.length) fcnBuffer.append(", ")
		  		})

		  		val closingParen : String = s")"
		  		fcnBuffer.append(closingParen)
		  		fcnBuffer.toString
			}
		}
		compoundPredStr
	}
	
}

class xSimplePredicate(val field : String, val operator : String, value : String) extends PmmlExecNode("SimplePredicate") {
  
	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xSimplePredicate($field $operator $value)"
		ext
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
	  	val fcnBuffer : StringBuilder = new StringBuilder()
		val simplePredStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				val opFcn : String = PmmlTypes.scalaBuiltinNameFcnSelector(operator)
				val sPred = s"$opFcn("
				fcnBuffer.append(sPred)
				
				val fldRef : String = PmmlExecNode.prepareFieldReference(ctx, field)
				
				/** getFieldType answers an array of the field types for the given field and a boolean indicating if it is a container type.*/
				val scalaTypes : Array[(String,Boolean, BaseTypeDef)] = ctx.getFieldType(field, false) 
				val scalaType : String = scalaTypes(0)._1
				val quotes : String = scalaType match {
					case "String" | "LocalDate" | "LocalTime" | "DateTime" => s"${'"'}"
					case _ => ""
				} 
				val fieldRefConstPair : String = s"$fldRef,$quotes$value$quotes"
				fcnBuffer.append(fieldRefConstPair)
		  		val closingParen : String = s")"
		  		fcnBuffer.append(closingParen)
		  		fcnBuffer.toString
			}
		}
		simplePredStr
	}
}

/**
  <SimpleSetPredicate field="art" booleanOperator="isIn">
      <Array n="1" type="string">0</Array>
   </SimpleSetPredicate> 
 
 */
class xSimpleSetPredicate(val field: String, val booleanOperator: String) extends PmmlExecNode("SimpleSetPredicate") {

	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xSimpleSetPredicate($field $booleanOperator {...})"
		ext
  	}

  	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
  	{
	  	val fcnBuffer : StringBuilder = new StringBuilder()
		val simplePredStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				val opFcn : String = PmmlTypes.scalaBuiltinNameFcnSelector(booleanOperator)
				val sPred = s"$opFcn("
				fcnBuffer.append(sPred)
				var cnt = 0
				Children.foreach((child : PmmlExecNode) => {
			  		generator.generateCode1(child.asInstanceOf[Option[PmmlExecNode]], fcnBuffer, generator, CodeFragment.FUNCCALL)
			  		cnt += 1
			  		if (cnt < Children.length) { 
			  			fcnBuffer.append(", ")
			  		}
		  		})
		  		val closingParen : String = s")\n"
		  		fcnBuffer.append(closingParen)
		  		fcnBuffer.toString
			}
		}
		simplePredStr
	}
}

class xArray(val n: String, val arrayType : String, val array : Array[String]) extends PmmlExecNode("Array") {

	override def asString(ctx : PmmlContext) : String = {
		val s = array.toString()
		val ext = s"xArray({$s})"
		ext
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, kind : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		var arrayStr : String = ""
		order match {
			case Traversal.INORDER => { "INORDER Not Used" }
			case Traversal.POSTORDER => { "POSTORDER Not Used" }
			case Traversal.PREORDER => {			  
				val aBuffer : StringBuilder = new StringBuilder()
				val arrTyp : String = PmmlTypes.scalaDataType(arrayType)
				val quotes : String = arrTyp match {
					case "String" | "LocalDate" | "LocalTime" | "DateTime" => s"${'"'}"
					case _ => ""
				} 
				var i = 0
				for (itm <- array) { 
					i += 1
					val comma : String = if (i < array.length) ", " else ""
					aBuffer.append(s"$quotes$itm$quotes$comma")
				}
				
				arrayStr = kind match {
				  case CodeFragment.FUNCCALL =>	"List(" + aBuffer.toString +")"		   
				  case CodeFragment.VARDECL | CodeFragment.VALDECL => s"new ArrayBuffer[$arrTyp]($aBuffer.toString)"			  
				  case _ => "unsupported code fragment kind requested"				  
				}
			}
		}
		arrayStr
	}
}

class xValue(val value : String, val property : String) extends PmmlExecNode("Value") {
	
	override def asString(ctx : PmmlContext) : String = {
		val ext = s"xValue(value = $value, property = $property)"
		ext
	}

	override def codeGenerator(ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String =
	{
		val valu : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {
				s" new Value(${'"'}$value${'"'}, ${'"'}$property${'"'})"
			}
		}
		valu
	}
}



object PmmlExecNode extends LogTrait {

 

	def mkPmmlExecConstant(ctx : PmmlContext, c : PmmlConstant) : Option[xConstant] = {
		var const : xConstant = new xConstant(c.dataType, PmmlTypes.dataValueFromString(c.dataType, c.Value()))
		
		Some(const)
	}

	def mkPmmlExecHeader(ctx : PmmlContext, headerNode : PmmlHeader) : Option[xHeader] = {
		/** Collect the copyright and description if present for later use during code generation */
		ctx.pmmlTerms("Copyright") = Some(headerNode.copyright)
		ctx.pmmlTerms("Description") = Some(headerNode.description)
	  
		Some(new xHeader(headerNode.copyright, headerNode.description))
	}
	
	def mkPmmlExecApplication(ctx : PmmlContext, headerNode : PmmlApplication) : Option[PmmlExecNode] = {
		/** Collect the Application name and the version if it is present */
		ctx.pmmlTerms("ApplicationName") = Some(headerNode.name)
		ctx.pmmlTerms("Version") = Some(headerNode.version)

		/** update the header parent node with the application name and version, don't create node*/
		
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  var header : xHeader = top.asInstanceOf[xHeader]
			  header.ApplicationNameVersion(headerNode.name, headerNode.version)
		  }
		  case _ => None
		}
		
		None 
	}
	
	def mkPmmlExecDataDictionary(ctx : PmmlContext, node : PmmlDataDictionary) : Option[xDataDictionary] = {
		Some(new xDataDictionary())
	}
	
	def mkPmmlExecDataField(ctx : PmmlContext, d : PmmlDataField) : Option[xDataField] = {
		/** update the data dictionary parent node with the datafield; don't create node*/
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		var datafld : xDataField = null
		top match {
		  case Some(top) => {
			  var dict : xDataDictionary = top.asInstanceOf[xDataDictionary]
			  datafld = new xDataField(d.name, d.displayName, d.optype, d.dataType, d.taxonomy, d.isCyclic)
			  dict.add(datafld)
			  ctx.dDict(datafld.name) = datafld
			  if (d.Children.size > 0) {
				  /** pick up the values for DataFields */
				  d.Children.foreach( aNode => {
					  if (aNode.isInstanceOf[PmmlValue]) {
					  val vNode : PmmlValue = aNode.asInstanceOf[PmmlValue]
						  datafld.addValuePropertyPair(vNode.value, vNode.property)
					  }
				  })
			  }
		  }
		  case _ => None
		}
		None
	}
	
	def mkPmmlExecValue(ctx : PmmlContext, node : PmmlValue) : Option[xValue] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  if (top.isInstanceOf[xDataField]) {
				  var d : xDataField = top.asInstanceOf[xDataField]
				  d.addValuePropertyPair(node.value, node.property)
			  } else {
				  var df : xDerivedField = top.asInstanceOf[xDerivedField]
				  df.addValuePropertyPair(node.value, node.property)
			  }
		  }
		  case _ => None
		}
		None
	}

	def mkPmmlExecInterval(ctx : PmmlContext, node : PmmlInterval) : Option[PmmlExecNode] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
			case Some(top) => {
				if (top.isInstanceOf[xDataField]) {
					  var d : xDataField = top.asInstanceOf[xDataField]
					  d.continuousConstraints(d.leftMargin, d.rightMargin, d.closure)
				} else {
					  var df : xDerivedField = top.asInstanceOf[xDerivedField]
					  df.continuousConstraints(df.leftMargin, df.rightMargin, df.closure)
				}
			}
			case _ => None
		}
		None
	}

	
	def mkPmmlExecTransformationDictionary(ctx : PmmlContext, node : PmmlTransformationDictionary) : Option[xTransformationDictionary] = {
		Some(new xTransformationDictionary())
	}

	def mkPmmlExecDerivedField(ctx : PmmlContext, d : PmmlDerivedField) : Option[PmmlExecNode] = {
		/** update the data dictionary parent node with the datafield; don't create node*/
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		var fld : xDerivedField = null
		top match {
		  case Some(top) => {
			  	var dict : xTransformationDictionary = top.asInstanceOf[xTransformationDictionary]
			  	fld = new xDerivedField(d.name, d.displayName, d.optype, d.dataType)
				dict.add(fld)
				ctx.xDict(fld.name) = fld
		  }
		  case _ => None
		}
		if (fld == null) {
			None
		} else {
		  Some(fld)
		}
		
	}
	
	def mkPmmlExecDefineFunction(ctx : PmmlContext, node : PmmlDefineFunction) : Option[xDefineFunction] = {  
		Some(new xDefineFunction(node.name, node.optype, node.dataType))
	}
			
	def mkPmmlExecParameterField(ctx : PmmlContext, node : PmmlParameterField) : Option[xParameterField] = {  
		Some(new xParameterField(node.name, node.optype, node.dataType))
	}
			
	def mkPmmlExecApply(ctx : PmmlContext, node : PmmlApply) : Option[xApply] = {  
		Some(new xApply(node.function, node.mapMissingTo, node.invalidValueTreatment))
	}

	def mkPmmlExecFieldRef(ctx : PmmlContext, node : PmmlFieldRef) : Option[xFieldRef] = {
		Some(new xFieldRef(node.field, node.mapMissingTo))
	}
	
	def mkPmmlExecMapValues(ctx : PmmlContext, d : PmmlMapValues) : Option[PmmlExecNode] = {
		val typ = d.dataType
		val containerStyle = d.containerStyle
		val dataSrc = d.dataSource
		
		var mapvalues = if (containerStyle == "map") {
			if (dataSrc == "inline") {
				new xMapValuesMapInline(d.mapMissingTo, d.defaultValue, d.outputColumn, d.dataType, containerStyle, dataSrc)
			  
			} else { /** locator */
				new xMapValuesMapExternal(d.mapMissingTo, d.defaultValue, d.outputColumn, d.dataType, containerStyle, dataSrc) 
			}
		} else { /** array */
			if (dataSrc == "inline") {
				new xMapValuesArrayInline(d.mapMissingTo, d.defaultValue, d.outputColumn, d.dataType, containerStyle, dataSrc)
			} else { /** locator */
				new xMapValuesArrayExternal(d.mapMissingTo, d.defaultValue, d.outputColumn, d.dataType, containerStyle, dataSrc) 
			}
		}
		Some(mapvalues)
	}
	
	def mkPmmlExecFieldColumnPair(ctx : PmmlContext, d : PmmlFieldColumnPair) : Option[xFieldColumnPair] = {
		/** update the value map node with the field col pair; don't create node*/
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  	var mv :xMapValuesMap = top.asInstanceOf[xMapValuesMap]
				mv.addFieldColumnPair (d.field , d.column) 
		  }
		  case _ => None
		}
		None
	}
	
	def mkPmmlExecrow(ctx : PmmlContext, d : Pmmlrow) : Option[PmmlExecNode] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  	var mv :xMapValuesMap = top.asInstanceOf[xMapValuesMap]

				/** Each PmmlRow has children (in this implementation we expect two values...
				 *  a key and a value.  The value maps to the MapValues output and the 
				 *  there is but one key sine we represent the inline table as a Map.
				 *  NOTE that more than one key can be present according to the spec. 
				 *  For this revision, this is not supported.  Tuple2 only.
				 */
				val tuples : ArrayBuffer[Option[PmmlRowTuple]] = d.children.filter(_.isInstanceOf[PmmlRowTuple]).asInstanceOf[ArrayBuffer[Option[PmmlRowTuple]]]
		
				val valuelen = tuples.length
				if (valuelen == 2) {
					val t0 = tuples(0)
					val k : String = t0 match {
					  case Some(t0) => t0.Value()
					  case _ => ""
					}
					val t1 = tuples(1)
					t1 match {
					  case Some(t1) => {
						  if (k.length() > 0)
							  mv.add(k,t1.Value())
					  }
					  case _ => 
					}
				} else {
					PmmlError.logError(ctx, s"Currently only 2 tuple rows (simple maps) are supported for InlineTables... row has $valuelen elements")
				}
		  }
		  case _ => None
		}
		None
	}
	
	def mkPmmlExecTableLocator(ctx : PmmlContext, node : PmmlTableLocator) : Option[PmmlExecNode] = {
		val pmmlxtensions : Queue[Option[PmmlExtension]] = node.children.filter(_.isInstanceOf[PmmlExtension]).asInstanceOf[Queue[Option[PmmlExtension]]]

		if (pmmlxtensions.length > 0) {
			val extens  = pmmlxtensions.map(x => {
				x match {
				  	case Some(x) => new xExtension(x.extender, x.name, x.value)
				  	case _ => new xExtension("", "", "")
				}
			}).toArray
		  
			val extensions = extens.filter(_.name.length > 0)
			if (! ctx.pmmlExecNodeStack.isEmpty) {
				val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
				top match {
					case Some(top) => {
						top match {
						  	case xMap : xMapValuesMap => xMap.addExtensions(extensions) 
						  	case xArr : xMapValuesArray => xArr.addExtensions(extensions) 
						  	case _ => PmmlError.logError(ctx, "Parent of TableLocator is not a kind MapValues...discarded")
						}
					}
					case _ => PmmlError.logError(ctx, "TableLocator cannot be added")
				}
			}
		}
	  
		None
	} 

	def mkPmmlExecInlineTable(ctx : PmmlContext, node : PmmlInlineTable) : Option[PmmlExecNode] = {
		val pmmlxtensions : Queue[Option[PmmlExtension]] = node.children.filter(_.isInstanceOf[PmmlExtension]).asInstanceOf[Queue[Option[PmmlExtension]]]

		if (pmmlxtensions.length > 0) {
			val extenss  = pmmlxtensions.map(x => {
				x match {
				  	case Some(x) => new xExtension(x.extender, x.name, x.value)
				  	case _ => new xExtension("", "", "")
				}
			}).toArray
			val extensions = extenss.filter(_.name.length > 0)
			if (! ctx.pmmlExecNodeStack.isEmpty) {
				val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
				top match {
					case Some(top) => {
						top match {
						  	case xMap : xMapValuesMap => xMap.addExtensions(extensions) 
						  	case xArr : xMapValuesArray => xArr.addExtensions(extensions) 
						  	case _ => PmmlError.logError(ctx, "Parent of InlineTable is not a kind MapValues...discarded")
						}
					}
					case _ => PmmlError.logError(ctx, "inline table cannot be added")
				}
			}
		}
		None
	} 

	def mkPmmlExecRuleSetModel(ctx : PmmlContext, node : PmmlRuleSetModel) : Option[xRuleSetModel] = {
		/** Gather content from RuleSetModel attributes for later use by the Scala code generator */
		ctx.pmmlTerms("ModelName") = Some(node.modelName)
		ctx.pmmlTerms("FunctionName") = Some(node.functionName)
		
		Some(new xRuleSetModel(node.modelName, node.functionName, node.algorithmName, node.isScorable))
	}
	
	def hlpMkMiningField(ctx : PmmlContext, d : PmmlMiningField) : xMiningField = {
		val name : String = d.name
		var fld : xMiningField = new xMiningField(d.name
											    , d.usageType
											    , d.optype
											    , 0.0
											    , d.outliers
											    , 0.0
											    , 0.0
											    , d.missingValueReplacement
											    , d.missingValueTreatment
											    , d.invalidValueTreatment)
		try {
			fld.Importance(d.importance.toDouble)
			fld.LowValue(d.lowValue.toDouble)
			fld.HighValue(d.highValue.toDouble)
		} catch {
			case _ : Throwable => ctx.logger.debug (s"Unable to coerce one or more of the mining field doubles... name = $name")
		}
	  	fld
	}
	
	def mkPmmlExecMiningSchema(ctx : PmmlContext, s : PmmlMiningSchema) : Option[PmmlExecNode] = {
		s.Children.foreach((child) => {
			val ch = child.asInstanceOf[PmmlMiningField]
			mkPmmlExecMiningField(ctx, ch)
		})
		None
	}

	def mkPmmlExecMiningField(ctx : PmmlContext, d : PmmlMiningField) : Option[xMiningField] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  	var mf : xRuleSetModel = top.asInstanceOf[xRuleSetModel]
				mf.addMiningField (d.name , hlpMkMiningField(ctx, d)) 
		  }
		  case _ => None
		}
		None
	}
	
	def mkPmmlExecRuleSet(ctx : PmmlContext, prs : PmmlRuleSet) : Option[xRuleSet] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  	var rsm : xRuleSetModel = top.asInstanceOf[xRuleSetModel]
				rsm.DefaultScore(prs.defaultScore)
		  }
		  case _ => None
		}
		Some(new xRuleSet(prs.recordCount, prs.nbCorrect, prs.defaultScore, prs.defaultConfidence))
	}
	
	
	def mkPmmlExecSimpleRule(ctx : PmmlContext, d : PmmlSimpleRule) : Option[PmmlExecNode] = {
		var rsm : Option[xRuleSetModel] = ctx.pmmlExecNodeStack.apply(1).asInstanceOf[Option[xRuleSetModel]]
		
		val id : Option[String] = Some(d.id)
		var rule : xSimpleRule = new xSimpleRule( id
													    , d.score
													    , 0.0 /** recordCount */
													    , 0.0 /** nbCorrect */
													    , 0.0 /** confidence */
													    , 0.0) /** weight */
		rsm match {
			case Some(rsm) => {
				
				try {
					rule.RecordCount(d.recordCount.toDouble)
					rule.CorrectCount(d.nbCorrect.toDouble)
					rule.Confidence(d.confidence.toDouble)
					rule.Weight(d.weight.toDouble)
				} catch {
					case _ : Throwable => ctx.logger.debug (s"Unable to coerce one or more mining 'double' fields... name = $id")
				}
			
				rsm.addRule (rule) 
			}
			case _ => None
		}
		Some(rule)
	}
	
	def mkPmmlExecScoreDistribution(ctx : PmmlContext, d : PmmlScoreDistribution) : Option[PmmlExecNode] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
		top match {
		  case Some(top) => {
			  	var mf : xSimpleRule = ctx.pmmlExecNodeStack.top.asInstanceOf[xSimpleRule]
				var sd : xScoreDistribution = new xScoreDistribution(d.value
															, 0.0 /** recordCount */
														    , 0.0 /** confidence */
														    , 0.0) /** probability */
				try {
					sd.RecordCount(d.recordCount.toDouble)
					sd.Confidence(d.confidence.toDouble)
					sd.Probability(d.probability.toDouble)
				} catch {
				  case _ : Throwable => ctx.logger.debug ("Unable to coerce one or more score probablity Double values")
				}
					
				mf.addScoreDistribution(sd)
		  }
		  case _ => None
		}
		None
	}
	
	def mkPmmlExecRuleSelectionMethod(ctx : PmmlContext, d : PmmlRuleSelectionMethod) : Option[PmmlExecNode] = {
		val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.apply(1)   // xRuleSetModel is the grandparent
		top match {
		  case Some(top) => {
			  	var mf : xRuleSetModel = top.asInstanceOf[xRuleSetModel]
			  	var rsm : xRuleSelectionMethod = new xRuleSelectionMethod(d.criterion)
				mf.addRuleSetSelectionMethod(rsm)
		  }
		  case _ => None
		}
		None
	}

	def mkPmmlExecArray(ctx : PmmlContext, a : PmmlArray) : Option[xArray] = {
		val valStr : String = a.valueString
		val elements = valStr.split(' ').toArray
		var trimmedElems  = elements.map (e => e.trim)
		val arrayPattern = """\"(.*)\"""".r
		
		val sanQuotesTrimmedElems = trimmedElems.map { x =>
			val matched = arrayPattern.findFirstMatchIn(x)
			matched match {
			  case Some(m) => m.group(1)
			  case _ => x
			}
		}
 		
		var arr : xArray = new xArray(a.n, a.arrayType, sanQuotesTrimmedElems)
		Some(arr)
	}

	def mkPmmlExecSimplePredicate(ctx : PmmlContext, d : PmmlSimplePredicate) : Option[xSimplePredicate] = {  
		Some(new xSimplePredicate(d.field, d.operator, d.value))
	}
	
	def mkPmmlExecSimpleSetPredicate(ctx : PmmlContext, d : PmmlSimpleSetPredicate) : Option[xSimpleSetPredicate] = {
		Some(new xSimpleSetPredicate(d.field, d.booleanOperator))
	}
	
	def mkPmmlExecCompoundPredicate(ctx : PmmlContext, d : PmmlCompoundPredicate) : Option[xCompoundPredicate] = {
		Some(new xCompoundPredicate(d.booleanOperator))
	}
	
	
	
	

	def prepareFieldReference(ctx : PmmlContext, field : String, order : Traversal.Order = Traversal.PREORDER) : String = {

		val fieldRefStr : String = order match {
			case Traversal.INORDER => { "" }
			case Traversal.POSTORDER => { "" }
			case Traversal.PREORDER => {

				/** prepare a field usage reference for the supplied field */
			  
				val fieldNodes : Array[String] = field.split('.')
				val possibleContainer : String = fieldNodes.head
				val expandCompoundFieldTypes : Boolean = true
				/** See the types for the containers with this to educate ourselves about it */
				val explodedFieldTypes : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(field, expandCompoundFieldTypes)

				/** Verify outer level container is a defined container in the pmml... */
				val isContainerInScope : Boolean = ctx.containersInScope.filter( tup4 => {
					possibleContainer == tup4._1 
				}).length > 0
				if (isContainerInScope) {
					val containerInfo : Option[(String, Boolean, BaseTypeDef, String)] = ctx.containersInScope.find( p => p._1 == possibleContainer)

					val (container, typeStr) : (BaseTypeDef, String) = containerInfo match {
					  case Some(containerInfo) => (containerInfo._3, containerInfo._3.typeString)    //containerType.typeString)
					  case _ => (null,"")
					}
					if (typeStr != "") {
						val varRefBuffer : StringBuilder = new StringBuilder
						val dataValueType : String = PmmlTypes.scalaTypeToDataValueType(typeStr)
						varRefBuffer.append(s"ctx.valueFor(${'"'}$possibleContainer${'"'}).asInstanceOf[$dataValueType]")
						/** Handle arbitrarily nested fields e.g., container.sub.subsub.field */
						if (fieldNodes.size > 1) { 
							val containerField : String= fieldNodes(1)
							val fieldsContainerType : ContainerTypeDef = container.asInstanceOf[ContainerTypeDef]
							
							/** lastContainer is non null when explodedFieldTypes.last is a container type.  It is stacked for use for determining field dereference 
							 *  expression print... either a fixed print ( container.fld ) or "mapped" print ( container.get("fld") )
							 */
							val containerFieldRep : String = compoundFieldPrint(fieldsContainerType, fieldNodes.tail, explodedFieldTypes.tail)

							varRefBuffer.append(s".Value.asInstanceOf[$typeStr]$containerFieldRep")
						} else {
							varRefBuffer.append(s".Value.asInstanceOf[$typeStr]")
						}
						
						varRefBuffer.toString
		
					} else {
						field
					}
				} else {
					/** 
					 *  With the container dereference case handled above, we can concern ourselves with only
					 *  the simple fields and maps or arrays of same. Ask the context to determine the type
					 *  and generate the correct coercion string suffix for the field access.
					 *  
					 *  FIXME: derived variables can also be dereferenced.  
					 */
					val expandCompoundFieldTypes : Boolean = true
					val typeStrs : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(field, ! expandCompoundFieldTypes)
					if (typeStrs != null && typeStrs.size > 0) {
						val typeStr : String = typeStrs(0)._1
						if (typeStr != "Unknown") {
							val dataValueType : String = PmmlTypes.scalaTypeToDataValueType(typeStr)
							if (dataValueType == "AnyDataValue")
								s"ctx.valueFor(${'"'}$field${'"'}).asInstanceOf[$dataValueType].Value.asInstanceOf[$typeStr]"
							else
								s"ctx.valueFor(${'"'}$field${'"'}).asInstanceOf[$dataValueType].Value"
						} else {
							PmmlError.logError(ctx, "Field '$field' is not known in either the data or transaction dictionary... fix this ")
							s"ctx.valueFor(${'"'}$field${'"'})"
						}
					} else {
						PmmlError.logError(ctx, s"Field reference '$field' did not produce any type(s)... is it mis-spelled?")
						
						s"UNKNOWN FIELD $field"
					}
				}
			}
		}
		fieldRefStr
	}

	/**
		Print the remaining field nodes in the supplied array, coercing them to their corresponding type.  The outer
		field node (a variable in one of the pmml dictionaries) is handled by the caller.	For the container types,
		generate the appropriate mapped (.apply(field)) or fixed (.field) access expression	
						
		@param fieldsContainerType the container type that contains this field, used to decide field dereference
			syntax (mapped vs. fixed)
		@param fieldNodes an array of the names found in a container qualified field reference (e.g., the 
			container.sub.subsub.fld of a field reference like msg.container.sub.subsub.fld)
		@param the type information corresponding to the fieldNodes.
		@return a string rep for the compound field
							
	 */	
						
	def compoundFieldPrint(fieldsContainerType : ContainerTypeDef
						, fieldNodes : Array[String]
						, explodedFieldTypes : Array[(String,Boolean,BaseTypeDef)]) : String = {
		val varRefBuffer : StringBuilder = new StringBuilder
		 
		fieldNodes.zip(explodedFieldTypes).foreach ( tuple => {
			val (field, typeInfo) : (String, (String,Boolean,BaseTypeDef)) = tuple
			val (typeStr, isContainerWFields, baseType) : (String,Boolean,BaseTypeDef) = typeInfo
			
			val isMappedContainer : Boolean = if (baseType != null) baseType.isInstanceOf[MappedMsgTypeDef] else false
			if (isMappedContainer) {
				varRefBuffer.append(s".get(${'"'}$field${'"'}).asInstanceOf[$typeStr]")			  
			} else {
				if (! fieldsContainerType.IsFixed) {
					varRefBuffer.append(s".get(${'"'}$field${'"'}).asInstanceOf[$typeStr]")
				} else {
					varRefBuffer.append(s".$field")
				}
			}
		})
		
		val container : ContainerTypeDef = if (explodedFieldTypes.last._3 != null && explodedFieldTypes.last._3.isInstanceOf[ContainerTypeDef]) {
			explodedFieldTypes.last._3.asInstanceOf[ContainerTypeDef]
		} else {
			null
		}
			
		varRefBuffer.toString
	}
		


}



