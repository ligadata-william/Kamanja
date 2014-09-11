package com.ligadata.Compiler

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._

/**
 * class MacroSelect retrieves a MacroDef from the mdmgr based upon the function signature.
 * Since the Macro is so similar to the function, a FunctionSelect is supplied to service
 * key formation and relaxation.
 */
class MacroSelect(val ctx : PmmlContext, val mgr : MdMgr, val node : xApply,generator : PmmlModelGenerator, val fcnSelector : FunctionSelect) {
							    
  
	def selectMacro : (MacroDef, Array[(String,Boolean,BaseTypeDef)]) = {
	  	/** create a search key from the function name and its children (the arguments). */
	  	val expandingContainerFields = true
	  	/** the typestring, isContainer flag, and metadata element for each argument is returned */
	  	var argTypes : Array[(String,Boolean,BaseTypeDef)] = fcnSelector.collectArgKeys(expandingContainerFields)
	  	var simpleKey : String = fcnSelector.buildSimpleKey(argTypes.map( argPair => argPair._1))
	  	val nmspcsSearched : String = ctx.NameSpaceSearchPath
	  	ctx.logger.trace(s"selectMacro ... key used for mdmgr search = '$nmspcsSearched.$simpleKey'...")
	  	var macroDef : MacroDef = ctx.MetadataHelper.MacroByTypeSig(simpleKey)
	  	if (macroDef == null) {
	  		val simpleKeysToTry : Array[String] = fcnSelector.relaxSimpleKey(argTypes)
	  		breakable {
	  		  	simpleKeysToTry.foreach( key => {
	  		  		ctx.logger.trace(s"selectMacro ...searching mdmgr with a relaxed key ... $key")
	  		  		macroDef = ctx.MetadataHelper.MacroByTypeSig(key)
	  		  		if (macroDef != null) {
	  		  			ctx.logger.trace(s"selectMacro ...found macroDef with $key")
	  		  			break
	  		  		}
	  		  	})	  		  
	  		}
	  	}
	  	val foundDef : String = if (macroDef != null)  "YES" else "NO!!"
	  	ctx.logger.trace(s"selectMacro ...macroDef produced?  $foundDef")
	  	(macroDef, argTypes)
	}
	
	/**
	 * 	Generate possibly two snippets of code, based upon the macro features that are specified
	 *  in the macro definition:
	 *  
	 *  <ulist>
	 *  <li>the 'builds' snippet</li>
	 *  <p>The 'builds' snippet is generated when CLASSUPDATE (and possibly other) features are specified in the MacroDef.
	 *  The builds snippet will contain the appropriate macro tempate instance fully spliced with the values required from
	 *  the argTypes and the xApply node supplied at the constructor time.  The arguments are positional with %1% corresponding to
	 *  argTypes(0), etc.   The appropriate macro is determined based upon whether there are containers present that require
	 *  a container.field treatment or a container(field) treatment.  In other words, is the container "fixed" or "mapped"?
	 *  This code snippet is enqueued for insertion into the code buffer just before the end of the compilation unit being built...
	 *  namely a derived field class.</p>
	 *  <li>the 'does' snippet</li>
	 *  <p>The 'does' snippet is always generated.  This snippet is inserted directly into the code buffer... at the current point.
	 *  Should this macro have the class update feature, this snippet will be responsible for an instantiation of the class and
	 *  execution of the method (the xApply node.function) enclosed in it (e.g., new someClass(...).incrementBy ...).</p>
	 *  </ulist>
	 *  
	 *  Pre-conditions:
	 *  <ulist>
	 *  <li>There is no provision currently for handling a macro that has BOTH a "fixed" and "mapped" container field at the same
	 *  time.</li>
	 *  </ulist>
	 *  
	 *  @param macroDef a MacroDef that describes the macro to be used for this code generation.
	 *  @param argTypes the function macro arg types and whether they are containers, (and functions too eventually).  It may be useful
	 *  	to return the actual type metadata in the second arg instead of a boolean or in addition to the boolean.
	 *  @return the "builds/does" pair as described above.  If there is no "builds" required, either an empty string or a null 
	 *  	string is returned in that position.
	 *  	
	 *  
	 */
	def generateCode(macroDef : MacroDef, argTypes : Array[(String,Boolean,BaseTypeDef)]) : (String,String) = {
	  
  		if (macroDef.features.contains(FcnMacroAttr.CLASSUPDATE)) {
  			generateClassBuildsAndDoes(macroDef, argTypes)
  		} else {
  			generateOnlyDoes(macroDef, argTypes)
  		}
	}
  	
	/** 
	 *  Some macros don't generate the "builds" class.  They emit some elaborate code to be inserted inline in the 
	 *  current function's string builder buffer.  To get properly handled, only the 2nd item in the returned 
	 *  tuple ... the one for the "does" piece ... is filled.  The "builds" item must be null to prevent 
	 *  the enqueue of text that at minimum could hammer the formatting for the derived field's class.
	 *  
	 *  @param macroDef a MacroDef that describes the macro to be used for this code generation.
	 *  @param argTypes the function macro arg types and whether they are containers, (and functions too eventually).  It may be useful
	 *  to return the actual type metadata in the second arg instead of a boolean or in addition to the boolean.
	 *  @return a "builds/does" pair with a null first tuple value.
	 */
	def generateOnlyDoes(macroDef : MacroDef, argTypes : Array[(String,Boolean,BaseTypeDef)]) : (String,String) = {
		(null,"")
	}
	
	/** 
	 *  A FcnMacroAttr.CLASSUPDATE featured macro has been supplied.  Create the "builds" and "does" portion of the
	 *  macro generation and return as a tuple.
	 *  
	 *  @param macroDef a MacroDef with CLASSUPDATE feature
	 *  @param argTypes an array of triples - one per argument supplied to the macro function in the pmml - where
	 *  	the values are (typeString representation, isAContainer, the BaseTypeDef for the argument)
	 *   
	 *   NOTE: The BaseTypeDef can be a typedef for any of the types or a function definition.  When it is a function
	 *   definition, the typestring is the result type string representation
	 */
	def generateClassBuildsAndDoes(macroDef : MacroDef, argTypes : Array[(String,Boolean,BaseTypeDef)]) : (String,String) = {
	  	/** 
	  	 *  Prepare the print representations for each node and assign it to the variables.  Variable
	  	 *  assignment is $1 for first argument, $1.type for its type, $2 for second argument, $2.type
	  	 *  for the its type, etc.
	  	 *  
	  	 *  We will pick and choose from them in that there may be container.field xFieldRef that have been split
	  	 *  into container and field
	  	 */
	  	
	  	val fcnBuffer : StringBuilder = new StringBuilder
		var fcnArgValues : ArrayBuffer[(String,String,Boolean)] = ArrayBuffer[(String,String,Boolean)]()
		var fcnArgTypes : ArrayBuffer[String] = ArrayBuffer[String]()
  		fcnBuffer.clear
  		var idx : Int = 0
  		val childCnt : Int = node.Children.size
  		val argTypesCnt : Int = argTypes.size
  		val reasonable : Boolean = (childCnt <= argTypesCnt)
  		val containerDotAddressingPresent : Boolean = (childCnt < argTypesCnt)
  		
  		/** determine which template should be used... either the "fixed" or the "mapped" */
  		val isFixed : Boolean = if (containerDotAddressingPresent) {
  			(argTypes.filter(triple => {
  				val (argTypeStr, isContainer, argElem) : (String,Boolean,BaseTypeDef) = triple
  				(isContainer && argElem.isInstanceOf[ContainerTypeDef] && argElem.asInstanceOf[ContainerTypeDef].IsFixed)
  			}).size > 0)
  		} else {
  			false
  		}
	  	
	  	/** Paw through the children and the argTypes supplied (considered together) to find the correct argument names
	  	 *  and types for the "builds" portion of the generation.  Notice that both the declarative "builds" name and whatever 
	  	 *  the runtime "does" expression string are collected.  Except for the container.field cases, the 2nd expression string
	  	 *  in the tuple is used in the "does" string formation that is immediately inserted into the derived field's execute
	  	 *  function printer.  For the containers, we need to regenerate a variable extraction for just the container name.
	  	 */
  		node.Children.foreach((child) => {
  			val (argTypeStr, isContainer, argElem) : (String,Boolean,BaseTypeDef) = argTypes(idx)
	  		generator.generateCode1(Some(child), fcnBuffer, generator, CodeFragment.FUNCCALL)
	  		val argPrint : String = fcnBuffer.toString
	  		/** Note: to support multiple level containers ... e.g., container.container.field... recursion needs to be introduced here */
	  		if (isContainer && child.asInstanceOf[xFieldRef].field.contains('.')) {
	  			val container : ContainerTypeDef = argElem.asInstanceOf[ContainerTypeDef]
	  			val compoundNm : Array[String] = child.asInstanceOf[xFieldRef].field.split('.')
	  			val containerName : String = compoundNm(0)
	  			val fieldName : String = compoundNm(1)
	  			fcnArgValues += Tuple3(containerName,containerName,true)
	  			fcnArgTypes += argTypeStr
	  			idx += 1
	  			val (fldArgType, isFldAContainer, fldElem) : (String,Boolean,BaseTypeDef) = argTypes(idx)
	  			fcnArgValues += Tuple3(fieldName,fieldName,false)
	  			fcnArgTypes += fldArgType
	  		} else {
	  			val (argNam,argTyp) : (String,String) = if (child.isInstanceOf[xConstant]) {
	  							val uniqNo : Int = ctx.Counter()
			  					val constNm = "value" + uniqNo.toString 
			  					(constNm,argTypeStr)
				  			} else {
				  				if (child.isInstanceOf[xApply]) {
									val fcnArgNm = child.asInstanceOf[xApply].function + "Result" +  idx.toString
									(fcnArgNm, argTypeStr)
					  			} else {
					  			    if (isContainer) {
				  			    		val containerType : ContainerTypeDef = if (argElem.isInstanceOf[ContainerTypeDef]) argElem.asInstanceOf[ContainerTypeDef] else null
		  			    				val containerName : String = child.asInstanceOf[xFieldRef].field
		  			    				(containerName, argTypeStr)
					  			    } else {
					  			    	if (child.isInstanceOf[xFieldRef]) {
					  			    		val fld : String = child.asInstanceOf[xFieldRef].field
					  			    		(fld, argTypeStr)
					  			    	} else {
					  			    		(argPrint, argTypeStr)
					  			    	}
					  			    }
					  			}
				  			}
	  			fcnArgValues += Tuple3(argNam,argPrint,false)
	  			fcnArgTypes += argTyp
	  		}
  			idx += 1
	  		fcnBuffer.clear
  		})
 	  
  		/** debugging checks */
  		val sizeOfArgVals : Int = fcnArgValues.size
  		val sizeOfArgTypes : Int = fcnArgTypes.size
  		val sameAmtArgsAsTypes : Boolean = (sizeOfArgVals == sizeOfArgTypes)
  		
  		/** construct the substitution map for the builds part */
  		var substitutionMap : HashMap[String,String] = HashMap[String,String]()
  		val argFcnMap = fcnArgValues zip fcnArgTypes
  		argFcnMap.zipWithIndex.foreach( pair => {
  			val (argval, argtype) = pair._1
  			val argidx : Int = pair._2 + 1
  			val argIdxName = argidx.toString 
  			val argName : String = argval._1
  			val argIdxTypeName = argidx.toString + "_type"
  			ctx.logger.trace(s"%$argIdxName% = $argName, %$argIdxTypeName% = $argtype)")
  			substitutionMap += s"%$argIdxName%" -> s"$argName"
  			substitutionMap += s"%$argIdxTypeName%" -> s"$argtype"
  		})
  		
  		/** get the fixed template */
  		val buildsTemplate : String = macroDef.macroTemplate._1
  		/** get the mapped template */
  		val mappedBuildsTemplate : String = macroDef.macroTemplate._2
  		
  		val (substitutedClassExpr,templateUsed) : (String,String) = if (isFixed) {
   			val subExpr : String = ctx.fcnSubstitute.makeSubstitutions(buildsTemplate, substitutionMap)
   			(subExpr,buildsTemplate)
  		} else {
  			val subExpr : String = ctx.fcnSubstitute.makeSubstitutions(mappedBuildsTemplate, substitutionMap)
   			(subExpr,buildsTemplate)
  		}
		ctx.logger.trace("builds class to be queued:")
		ctx.logger.trace(s"\n$substitutedClassExpr")		  
		
		/** 
		 *	Create the "does" portion 
		 *  	a) Get generated class name from generated string.  
		 *   	b) From the template, get the parameter indices needed in the arg list for our "does" cmd
		 *    	c) construct the argument list accordingly
		 */
		val classUpdateClassName : String = getClassName(substitutedClassExpr)
		val parametersUsed : Array[Int] = getArgParameterIndices(templateUsed)
		val doesCmd : String = generateDoes(macroDef, argTypes, fcnArgValues, classUpdateClassName, parametersUsed)

		ctx.logger.trace("'does' use of 'builds' class to be used:")
		ctx.logger.trace(s"\n$doesCmd")		  
		
	  
	  	(substitutedClassExpr,doesCmd)
	}
	
	/** 
	 *  Answer the "does" string... the one that will be inserted into the current function's write buffer.  The content of that string
	 *  will instantiate the CLASSUPDATE class that will be inserted just before the closing brace of the derived field enclosing this function.
	 *  
	 *  @param macroDef the function macro that defines the metadata particulars for the current xApply's function (i.e., the one used to construct
	 *  	this MacroSelect instance).
	 *  @param argTypes an array of triples - one per argument supplied to the macro function in the pmml - where
	 *  	the values are (typeString representation, isAContainer, the BaseTypeDef for the argument)
	 *  @param fcnArgValues - an ArrayBuffer of triples where a) the first string is the declared variable name that was crafted during creation of
	 *  	the class that will handle the UPDATE job this function macro to invoke, b) the second string is the what the standard generate 
	 *   	function produced; this will be used in most cases to produce the arguments for the "does" snippet to be produced here, and c) the 
	 *    	boolean flag as to whether the argument was one that is a Container.field pattern.  When that is the case, the standard generate
	 *     	text is incorrect because it is fetching the field content, not the reference to the container, the field's parent needed for the
	 *      update.
	 *  @param classUpdateClassName - this is the generated class name produced in (e.g.,...) generateClassBuildsAndDoes.  It will be part of 
	 *  	string to be generated here "
	 *  @param parameterIndices - these are indices into the fcnArgValues parameter (the 2nd arg) that are needed for the "does" arguments to 
	 *  	be produced here.
	 *  @return the generated use of the class that was generated by the "builds" method .
	 */
	def generateDoes(macroDef : MacroDef, argTypes : Array[(String,Boolean,BaseTypeDef)], fcnArgValues : ArrayBuffer[(String,String,Boolean)], classUpdateClassName : String, parameterIndices : Array[Int]) : String = {
		val buffer : StringBuilder = new StringBuilder
		buffer.append(s"new $classUpdateClassName(")
		var cnt : Int = 0
		val paramCnt : Int = parameterIndices.size
		if (argTypes.size == 5) {
			val stopHere = 0
		}
		parameterIndices.foreach( idx => {
			val (typStr, isCntnr, elemdef) : (String,Boolean,BaseTypeDef) = argTypes(idx)
			val (argName, exprStr, isContainer) : (String,String,Boolean) = fcnArgValues(idx)
			val childNode = if (! isContainer) node.Children.apply(cnt) else null
			if (isContainer) {
				val appropriateTypeArgs = argTypes(cnt)
				val (typeStr, isCtnr, elem) : (String,Boolean,BaseTypeDef) = appropriateTypeArgs
				buffer.append(s"ctx.valueFor(${'"'}$argName${'"'}).asInstanceOf[AnyDataValue].Value.asInstanceOf[$typeStr]")
			} else {
				if (childNode != null && childNode.isInstanceOf[xFieldRef] && !(elemdef.isInstanceOf[ContainerTypeDef])) {
					val argNameQuoted : String = if (argName.contains(s"${'"'}")) argName else s"${'"'}$argName${'"'}"
					buffer.append(argNameQuoted)
				} else {
					buffer.append(exprStr)
				}
			}
			cnt += 1
			if (cnt < paramCnt) {
				buffer.append(", ")
			}
		})
		/** 
		 *  By convention, the name of the function in the class that executes is the name of the function that appears 
		 *  in the pmml function name.  It is currently up to the macro writer to use the right name (i.e., the same
		 *  name that is used in the pmml apply function.
		 */
		val function : String = node.function
		buffer.append(s").$function")
		val doesString : String = buffer.toString
		doesString
	}
	
	/** 
	 *  From the substituted "builds" string, get the class name.  
	 *  
	 *  NOTE: It is assumed that the template starts with optional white
	 *  space and then the keyword class and then what will be the name
	 *  after substitution.
	 *  
	 *  This could be made more robust perhaps...
	 *  
	 */
	def getClassName(substitutedClassExpr : String) : String = {
		val firstPart : String = substitutedClassExpr.split('(').head.trim
		var className : String = firstPart.split("[ \t\r\n\f]+").last
		
		className
	}

	/** 
	 *  Collect the arguments that are used in the class parameter list from
	 *  the template and translate them to array indices.
	 */
	def getArgParameterIndices(templateUsed : String) : Array[Int] = {
	  	val numeric = "^([0-9]+)$".r
	  	val argsOnly : String = encloseArgs(templateUsed, '(', ')')
	  	val postionalArgNos : Array[String] = if (argsOnly != null) {
	  		val roughArray : Array[String] = argsOnly.split('%')
	  		roughArray.map( part => {
	  			var arg : String = null
	  			try {
		  			val numeric(argNum) = part
		  			arg = argNum
	  			} catch {
	  			  case e: Exception => arg = null
	  			}
	  			arg
	  		})
	  	} else {
	  	   Array[String]()
	  	}
	  	postionalArgNos.filter(arg => arg != null).map( num => num.toInt - 1)
	}

	def encloseArgs(typeString : String, openBracketOrParen : Char, closeBracketOrParen : Char) : String = {
		var begoffset : Int = 0
		var offset : Int = 0
		var bracketOrParenCnt : Int = 0
		var bracketOrParenSet : Boolean = false
		breakable {
			for (ch <- typeString) {			
				if (ch == openBracketOrParen) {
					if (! bracketOrParenSet) {
						begoffset = offset
						bracketOrParenSet = true
					}
					bracketOrParenCnt += 1 
				} else {
					if (ch == closeBracketOrParen) {
						bracketOrParenCnt -= 1
						if (bracketOrParenCnt == 0) {  /** find the close bracket or paren matching the first one */
							offset += 1
							break
						} 
					}
				}
				offset = offset + 1	  
			}
		}
		
		/** there has to be at least one character between the (...) of the typeString arg portion */
		val typeArgs : String = if (offset > 2) typeString.substring(begoffset + 1, offset - 1) else ""
		  
		typeArgs
	}
}

