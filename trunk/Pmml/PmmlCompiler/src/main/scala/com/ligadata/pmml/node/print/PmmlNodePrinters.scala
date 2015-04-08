package com.ligadata.pmml.node.print

import scala.collection.mutable._
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import org.apache.log4j.Logger
import com.ligadata.pmml.compiler._
import com.ligadata.pmml.support._
import com.ligadata.pmml.syntaxtree.cooked._
import com.ligadata.olep.metadata._
import com.ligadata.pmml.fcnmacro._

//import com.ligadata.Pmml.Runtime._


object NodePrinterHelpers extends com.ligadata.pmml.compiler.LogTrait {

	/**
	 *  Generate enumerated value representation for data and derived fields.  Scala dictionary expressions returned for
	 *  each value/property pair in the supplied array
	 *  
	 *  @param vals an ArrayBuffer of value/property pairs
	 *  @return a string
	 */
	def valuesHlp(vals : ArrayBuffer[(String,String)], valNm : String, pad : String) : String = {
		val valuBuf : StringBuilder = new StringBuilder()
		valuBuf.append(s" new ArrayBuffer[(String,String)]()")
		var i : Int = 0
   		vals.foreach((v) => {
   			val (value, property) : (String,String) = v 
   			if (i == 0)
   				valuBuf.append(s"\n$pad$valNm += (${'"'}$value${'"'} -> ${'"'}$property${'"'})\n")
   			else
   				valuBuf.append(s"$pad$valNm += (${'"'}$value${'"'} -> ${'"'}$property${'"'})\n")
   			i = i + 1
   				  
  		})
		valuBuf.toString
	}
	
	/** 
	 *  Manage the Scala generation for <Apply> elements.
	 *  
	 *  @param node an xApply to be printed
	 *  @param ctx the global PmmlContext
	 *  @param ctx the controlling model printer generator used when recursion needed
	 *  @param fragment kind an enumerated type describing the sort of fragment to be generated... one of 
	 *  	{VARDECL, VALDECL, FUNCCALL, DERIVEDCLASS, RULECLASS, RULESETCLASS , MININGFIELD, MAPVALUE, AGGREGATE, USERFUNCTION}
	 *  @param order the traversal order of this xApply tree... one of {INORDER, PREORDER, POSTORDER}
	 *  @return string representation for this tree and its children 
	 */

	def applyHelper(node : xApply, ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String = {
			
		val fcnBuffer : StringBuilder = new StringBuilder()
		var scalaFcnName : String = PmmlTypes.scalaBuiltinNameFcnSelector(node.function)
		
		val isPmmlBuiltin : Boolean = scalaFcnName match {
			case "Unknown Operator" =>  {
				/** This is one of our functions... use the name to get the pattern template */
				scalaFcnName = node.function
				false
			}
			case _ => true
		}
		
		val variadic : Boolean = ctx.MetadataHelper.FunctionsWithIndefiniteArity(scalaFcnName)
		
		/** 
		 *  There are four cases:
		 *  	1) "built in" function.  These are functions supported from the Pmml specification
		 *   	2) "iterable" function.  These are functions that have a collection (some type with "Iterable" interface.
		 *    		These functions are printed with the collection as the receiver (e.g., coll.map(mbr => ...))
		 *      3) "simple" function.  These are Udfs that require no special printing needs
		 *      4) "macro" function. The macros generate template specific code of two types:
		 *      	a) "does" code is a string returned from the macro printer that will print immediately inline
		 *       		in the current printer.
		 *         	b) "builds" code is a string that is printed out of line just before closure of the current class.
		 *          	See MacroSelect.scala for the details and use cases. 
		 */

		ctx.elementStack.push(node) /** track the element as it is processed */
		if (isPmmlBuiltin && ! variadic) {	/** pmml functions in the spec */
			/** Take the translated name (scalaFcnName) and print it */
			simpleFcnPrint(scalaFcnName
						, node
					    , ctx
					    , generator
					    , generate
					    , order
					    , fcnBuffer
					    , null)					
		} else { /** several of the builtins... or, and, etc... have variadic implementations... we want to go through FunctionSelect for them */
			var funcDef : FunctionDef = null
			val functionSelector : FunctionSelect = new FunctionSelect(ctx, ctx.mgr, node)
			val isIterable = functionSelector.isIterableFcn
			if (isIterable) { /** iterable functions in the form coll.<fcn>(mbr => ...) */
				val fcnTypeInfo :  FcnTypeInfo = functionSelector.selectIterableFcn 
				node.SetTypeInfo(fcnTypeInfo)
				
				val applysFcnName : String = node.function
				val iterablePrinter : IterableFcnPrinter = new IterableFcnPrinter(applysFcnName
																				, node
																			    , ctx
																			    , generator
																			    , generate
																			    , order
																			    , fcnTypeInfo)
				iterablePrinter.print(fcnBuffer)
				funcDef = fcnTypeInfo.fcnDef
				
			} else {  /** simple function...*/
				val fcnTypeInfo :  FcnTypeInfo = functionSelector.selectSimpleFcn
				if (fcnTypeInfo != null && fcnTypeInfo.fcnDef != null) {
					node.SetTypeInfo(fcnTypeInfo)
					//val scalaFcnName : String = node.function
					simpleFcnPrint(scalaFcnName
								, node
							    , ctx
							    , generator
							    , generate
							    , order
							    , fcnBuffer
							    , fcnTypeInfo.fcnDef)					
					funcDef = fcnTypeInfo.fcnDef
				}
			}
			if (funcDef == null) {
				/** perhaps its a macro instead */
				val macroSelector : MacroSelect = new MacroSelect(ctx, ctx.mgr, node, generator, functionSelector)
				val (macroDef,macroArgTypes) : (MacroDef, Array[(String,Boolean,BaseTypeDef)]) = macroSelector.selectMacro
				if (macroDef != null) {	  
					val (builds, does) : (String,String) = macroSelector.generateCode(macroDef, macroArgTypes)
					if (builds != null && builds.size > 0) {
						/** push this to the context's update class queue.  The PmmlNodePrinter's 
						 *  method, derivedFieldFcnHelper, will pull them from the queue and print them just before
						 *  the end of the derived field class generation emits the closed '}'
						 *  
						 *  In other words, these classes will be scoped to the derived field class that is generated.
						 */
						ctx.UpdateClassQueue.enqueue(builds)
					}
					/** as to the does part, this is inserted directly into the function string builder */
					fcnBuffer.append(does)
				  
				} else {  /** assume it is a function and issue some diagnostics */
			  
					val fcnName : String = node.function
					val fcnsThatWouldMatchWithRightTypeSig = functionSelector.FunctionKeysThatWouldMatch(fcnName)
					if (fcnsThatWouldMatchWithRightTypeSig.size > 0) {
						val matchFcnBuff : StringBuilder = new StringBuilder()
						matchFcnBuff.append("Any{")
						fcnsThatWouldMatchWithRightTypeSig.addString(matchFcnBuff, s"${'"'},${'"'}").append(s"${'"'}}")
						PmmlError.logError(ctx, s"Function '$fcnName' could not be located in the Metadata.")
						logger.error(s"One or more functions named '$fcnName' is/are known, but your key is not good enough to locate one")
						logger.error(s"Any of these keys would return a version of $fcnName:")
						val fcnSetStr : String = matchFcnBuff.toString
						logger.error(s"    $fcnSetStr")
					} else {
						PmmlError.logError(ctx, s"Function $fcnName is not known in the metadata...")
					}
				}
			} 
		}
		
		ctx.elementStack.pop 
		
		val fcnExprStr : String = fcnBuffer.toString
	  	fcnExprStr
	  
	}
	
		
	/** 
	 *	Print the function in a straight forward fashion.  The name is passed in here
	 *  so it can serve both the Builtin functions as well as ordinary functions that 
	 *  were detected in the Metadata. See usage in applyHelper above.
	 * 
	 *  Note: funcDef is not currently used, but may be incorporated into the navigation
	 *  so that type info for the function is available at each child... should type
	 *  coercion be needed.
	 */
	def simpleFcnPrint(scalaFcnName : String
					, node : xApply
				    , ctx : PmmlContext
				    , generator : PmmlModelGenerator
				    , generate : CodeFragment.Kind
				    , order : Traversal.Order
				    , fcnBuffer : StringBuilder
				    , funcDef : FunctionDef) : Unit = {
	  
		if (scalaFcnName == "if" || scalaFcnName == "and" || scalaFcnName == "or") {
			shortCircuitFcnPrint(scalaFcnName
					, node
				    , ctx
				    , generator
				    , generate
				    , order
				    , fcnBuffer
				    , funcDef)
		} else {
			val fcnName = s"$scalaFcnName("
			fcnBuffer.append(fcnName)
			
			val noChildren = node.Children.length
			var cnt = 0
			node.Children.foreach((child) => {
				cnt += 1	
				generator.generateCode1(Some(child), fcnBuffer, generator, CodeFragment.FUNCCALL)
		  		if (cnt < noChildren) {
		  			fcnBuffer.append(", ")
		  		}  
	  		})
	  		
	  		val closingParen : String = ")"
	  		fcnBuffer.append(closingParen)
	  		
	  		val fcnUseRep : String = fcnBuffer.toString
	  		//logger.trace(s"simpleFcnPrint ... fcn use : $fcnUseRep")
	  		val huh : String = "huh" // debugging rest stop 	
		}
	}

	/** 
	 *	Control printing of inline function which we wish to short circuit using scala
	 *  generation.  Currently 'and' and 'or' are supported.
	 */
	def shortCircuitFcnPrint(scalaFcnName : String
					, node : xApply
				    , ctx : PmmlContext
				    , generator : PmmlModelGenerator
				    , generate : CodeFragment.Kind
				    , order : Traversal.Order
				    , fcnBuffer : StringBuilder
				    , funcDef : FunctionDef) : Unit = {

		scalaFcnName match {
		  case "if" => {
			ifFcnPrint(scalaFcnName
					, node
				    , ctx
				    , generator
				    , generate
				    , order
				    , fcnBuffer
				    , funcDef)		    
		  }
		  case "and" => {
			andOrFcnPrint("&&"
					, node
				    , ctx
				    , generator
				    , generate
				    , order
				    , fcnBuffer
				    , funcDef)		    
		  }
		  case "or" => {
			andOrFcnPrint("||"
					, node
				    , ctx
				    , generator
				    , generate
				    , order
				    , fcnBuffer
				    , funcDef)		    
		  }
		}
	}
	
	/** 
	 *	Printing of either 'and' and 'or' variadic function expressions
	 * 
	 * 	@param scalaFcnName a string that is either "&&" or "||"
	 *  @param node the original PmmlExecNode describing the Pmml Apply element
	 *  @param ctx the PmmlCompiler context
	 *  @param the generator used to navigate the syntax tree print
	 *  @param order a hint to the generator as to which order to traverse the tree
	 *  @param fcnBuffer a StringBuilder that will contain the generated code upon exit
	 *  @param the function definition (currently not used)
	 *  
	 */
	def andOrFcnPrint(scalaFcnName : String
					, node : PmmlExecNode
				    , ctx : PmmlContext
				    , generator : PmmlModelGenerator
				    , generate : CodeFragment.Kind
				    , order : Traversal.Order
				    , fcnBuffer : StringBuilder
				    , funcDef : FunctionDef) : Unit = {
	  
		val andOrFcnBuffer : StringBuilder = new StringBuilder()
		
		val noChildren = node.Children.length
		var cnt = 0
		node.Children.foreach((child) => {
			cnt += 1	
			andOrFcnBuffer.append("(")
			generator.generateCode1(Some(child), andOrFcnBuffer, generator, CodeFragment.FUNCCALL)
			andOrFcnBuffer.append(")")
	  		if (cnt < noChildren) {
	  			andOrFcnBuffer.append(s" $scalaFcnName ")
	  		}  
  		})
  		
  		val andOrFcnUseRep : String = andOrFcnBuffer.toString
  		fcnBuffer.append(andOrFcnUseRep)
	}
	
	def ifFcnPrint(scalaFcnName : String
					, node : xApply
				    , ctx : PmmlContext
				    , generator : PmmlModelGenerator
				    , generate : CodeFragment.Kind
				    , order : Traversal.Order
				    , fcnBuffer : StringBuilder
				    , funcDef : FunctionDef) : Unit = {
	  
		val ifFcnBuffer : StringBuilder = new StringBuilder()
		
		val noChildren = node.Children.length
		if (node.function == "if" && noChildren != 3) {
			PmmlError.logError(ctx, s"if statement encountered that does not have 3 parts... a predicate, a true action and a false action.")
			logger.error(s"only functional form ('if (predicate) trueAction else falseAction') supported. ")
		}
		
		val predicate : PmmlExecNode = node.Children(0)
		val trueAction : PmmlExecNode = node.Children(1)
		val falseAction : PmmlExecNode = node.Children(2)
		
		ifFcnBuffer.append("if(")
		generator.generateCode1(Some(predicate), ifFcnBuffer, generator, CodeFragment.FUNCCALL)
		ifFcnBuffer.append(") {")
		generator.generateCode1(Some(trueAction), ifFcnBuffer, generator, CodeFragment.FUNCCALL)
		ifFcnBuffer.append("} else {")
		generator.generateCode1(Some(falseAction), ifFcnBuffer, generator, CodeFragment.FUNCCALL)
  		ifFcnBuffer.append("}")

  		val ifFcnUseRep : String = ifFcnBuffer.toString
  		fcnBuffer.append(ifFcnBuffer.toString)
	}
	
	/** 
		<SimpleRule id="RULE1" score="1">
			<CompoundPredicate booleanOperator="and">
				<SimplePredicate field="sex" operator="equal" value="1"/>
				<SimplePredicate field="hiRiskAsthmaMarker" operator="equal" value="1"/>
			</CompoundPredicate>
		</SimpleRule>
	
		class SimpleRule_01_1(id : Option[String]
				    , score : String
				    , recordCount : Double
				    , nbCorrect : Double
				    , confidence : Double
				    , weight : Double) extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {
		  
			override def execute(defaultScore : String) : Option[String] = {
				val answer = If (And(Equal(ctx.getValueFor("sex"), 1)), Equal(ctx.getValueFor(hiRiskAsthmaMarker),"1"))
				if (answer) Some(score) else {
					match defaultScore {
					  case Some(defaultScore) => defaultScore
					  case _ => None
					}
				}
			}
		}	 
	 */
	
	def simpleRuleHelper(node : xSimpleRule, ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String = {
			
		val clsBuffer : StringBuilder = new StringBuilder()
		val nmBuffer : StringBuilder = new StringBuilder()
		val fcnBuffer : StringBuilder = new StringBuilder()
		
		nmBuffer.append("SimpleRule_")
		node.id match {
		  case Some(id) => nmBuffer.append(id + "_0" + ctx.Counter().toString())
		  case _ => nmBuffer.append("NoId" + ctx.Counter().toString())
		}
		val classNm : String = nmBuffer.toString()
		val classNameFixer = "[-.]+".r
		val classname : String = classNameFixer.replaceAllIn(classNm,"_")		
		
		clsBuffer.append(s"class $classname (id : String, score : String, recordCount : Double, nbCorrect : Double, confidence : Double, weight : Double) \n")
		clsBuffer.append(s"      extends SimpleRule(id, score, recordCount, nbCorrect, confidence, weight) {\n")
		clsBuffer.append(s"    override def execute(ctx : Context, defaultScore : StringDataValue) : String = {\n")
		clsBuffer.append(s"        val answer : Boolean = ")
		
		node.Children.foreach((child) => {
	  		generator.generateCode1(Some(child), fcnBuffer, generator, CodeFragment.FUNCCALL)
  		})
  		clsBuffer.append(fcnBuffer.toString)
  		clsBuffer.append(s"\n")
  		
  		clsBuffer.append(s"        if (answer == true) score else defaultScore.Value\n")
  		clsBuffer.append(s"    }\n")
  		clsBuffer.append(s"}\n")

		val idStr : String = node.id match {
		  case Some(id) => id
		  case _ => "NoId"
		}
		val score : String = node.score
		val recordCount : Double = node.recordCount
		val nbCorrect : Double = node.nbCorrect
		val confidence : Double = node.confidence
		val weight : Double = node.weight
  		val instantiator : String = s"new $classNm(${'"'}$idStr${'"'}, ${'"'}$score${'"'}, $recordCount, $nbCorrect, $confidence, $weight)"
  		ctx.RuleRuleSetInstantiators.addBinding("SimpleRule", instantiator)
  		ctx.simpleRuleInsertionOrder += instantiator

  		clsBuffer.toString
	}
	
	
	/**
		class RuleSetModel_01_1(modelName : String
				, val functionName : String
				, val algorithmName : String
				, val isScorable : String) extends RuleSetModel(modelName, functionName, algorithmName, isScorable) {
		  
				override def execute(ctx : Context) : Option[String] = {
					val results : List[Option[String]] = RuleSet().map(rule => rule.execute())
					MakePrediction(ctx, results)
				}
			}
		}

	 */

	def ruleSetModelHelper(node : xRuleSetModel, ctx : PmmlContext, generator : PmmlModelGenerator, generate : CodeFragment.Kind, order : Traversal.Order = Traversal.PREORDER) : String = {
			
		val clsBuffer : StringBuilder = new StringBuilder()
		val fcnBuffer : StringBuilder = new StringBuilder()
		val serialNo : String = ctx.Counter().toString()
		val nodeName : String = node.qName                
		val nodeFcnName : String = node.functionName
		val className : String = s"RuleSetModel${'_'}$nodeFcnName${'_'}0$serialNo"
		val classNameFixer = "[-.]+".r
		val classname : String = classNameFixer.replaceAllIn(className,"_")

	  /**	
      	override def execute(ctx : Context) {
          val results : ArrayBuffer[String] = ArrayBuffer[String]()
          var res : String = "0"
          breakable {
              RuleSet().foreach(rule => {
                  res = rule.execute(ctx, DefaultScore)
                  if (res != "0") break 
                  results += res
              })
          }
          results += res
          MakePrediction(ctx, results)

		*/

		/** generate a break for firstHit selection method ONLY... so the remaining rules can be skipped. */
		val firstSelectMechanismPresent : Boolean = (node.ruleSetSelectionMethods.filter( selectMethod => selectMethod.criterion == "firstHit").size > 0)
		if (firstSelectMechanismPresent && node.ruleSetSelectionMethods.size == 1) {
			clsBuffer.append(s"class $classname (modelName : String, functionName : String, algorithmName : String, isScorable : String) \n")
			clsBuffer.append(s"      extends RuleSetModel(modelName, functionName, algorithmName, isScorable) { \n\n")
			clsBuffer.append(s"      override def execute(ctx : Context) {\n")
			clsBuffer.append(s"          var results : ArrayBuffer[String] = ArrayBuffer[String]()\n")
			clsBuffer.append(s"          var res : String = DefaultScore.Value\n")
			clsBuffer.append(s"          breakable {\n")
			clsBuffer.append(s"              RuleSet().foreach(rule => {\n")
			clsBuffer.append(s"                  res = rule.execute(ctx, DefaultScore)\n")
			clsBuffer.append(s"                  if (res != ${'"'}0${'"'}) break \n")
			clsBuffer.append(s"                  /**results += res*/\n")
			clsBuffer.append(s"              })\n")
	  		clsBuffer.append(s"          }\n")
			clsBuffer.append(s"          results += res\n")
			clsBuffer.append(s"          MakePrediction(ctx, results)\n")
	  		clsBuffer.append(s"      }\n")
	  		clsBuffer.append(s"}\n")
		} else {
			/** when the selection methods have weightedSum and/or weightedMax all rules will execute */ 
			clsBuffer.append(s"class $classname (modelName : String, functionName : String, algorithmName : String, isScorable : String) \n")
			clsBuffer.append(s"      extends RuleSetModel(modelName, functionName, algorithmName, isScorable) { \n\n")
			clsBuffer.append(s"      override def execute(ctx : Context) {\n")
			clsBuffer.append(s"          val results : ArrayBuffer[String] = RuleSet().map(rule => rule.execute(ctx, DefaultScore))\n")
			clsBuffer.append(s"          MakePrediction(ctx, results)\n")
	  		clsBuffer.append(s"      }\n")
	  		clsBuffer.append(s"}\n")
		}
		
  		/** remember a constructor string for the class, saving it for initialization method code generation in model subclass */
  		val modelName : String = node.modelName
  		val functionName : String = node.functionName
  		val algoName : String = node.algorithmName
  		val isScorable : String = node.isScorable
  		val instantiator : String = s"new $className(${'"'}$modelName${'"'}, ${'"'}$functionName${'"'}, ${'"'}$algoName${'"'}, ${'"'}$isScorable${'"'})"
  		ctx.RuleRuleSetInstantiators.addBinding("RuleSetModel", instantiator)

  		/** Remember all of the mining variables by capturing the set of statements to add mining field instances to the RuleSetModel that will
  		 *  be instantiated in the initialize function. */
  		node.MiningSchemaMap.foreach(fld => { 
  			val name = fld._2.name
  			val usageType = fld._2.usageType
  			val opType = fld._2.opType
  			val importance = fld._2.importance
  			val outliers = fld._2.outliers
  			val lowValue = fld._2.lowValue
  			val highValue = fld._2.highValue
  			val missingValueReplacement = fld._2.missingValueReplacement
  			val missingValueTreatment = fld._2.missingValueTreatment
  			val invalidValueTreatment = fld._2.invalidValueTreatment
  			
  			ctx.miningSchemaInstantiators += (fld._1 -> s"new MiningField(${'"'}$name${'"'},${'"'}$usageType${'"'},${'"'}$opType${'"'},$importance,${'"'}$outliers${'"'},$lowValue,$highValue,new StringDataValue(${'"'}$missingValueReplacement${'"'}),${'"'}$missingValueTreatment${'"'},${'"'}$invalidValueTreatment${'"'})")
  		})
	
   		clsBuffer.toString
	  
	}

	/**
	 	Categorized return types are treated by grabbing the categorized types from the last function in the child nodes (there is usually only one) 
	 	and creating a "categorize" function in the derived field class that is being generated.  Both the derived field and the top level function 
	 	should have the same type.  If the top level function is an 'if' apply then 1 and 0 are used as the keys for a case statement to pick up
	 	the value.  If the function returns something else, other sorts of mechanisms are needed (maps, discretization, etc )
	 	
	 	Example generation of hiRiskAsthmaMarker:

		class Derive_hiRiskAsthmaMarker (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) 
		      extends DerivedField(name, dataType, values, leftMargin, rightMargin, closure) { 
		
		    override def execute(ctx : Context) : Option[AnyDataValue] = {
		        val hiRiskAsthmaMarker = Builtins.If(Builtins.And(Builtins.GreaterThan(ctx.valueFor("inpatientClaimCost")), Builtins.GreaterThan(ctx.valueFor("outpatientClaimCost"))))
		        var result : String = if (hiRiskAsthmaMarker) "1" else "0"
		
		        Some(new AnyDataValue(result))
		    }
		}		

		Note: If the derived field's validValues array has elements (i.e., a categorized optype field value are constrained to these), it is currently assumed that the value
		is a Boolean and there are but two.  We need to think what the other cases would be.   For example, a value is "binned" to some range and the result associated with that
		range is returned.  This will become more apparent when the other models (regression, tree, et al are implemented... For RuleSet, for now,... if categorized, it is a boolean
		that is returned. 
	 */
	
	def derivedFieldClassSignature(clsBuffer : StringBuilder
							, node : xDerivedField
						    , ctx : PmmlContext
						    , generator : PmmlModelGenerator
						    , generate : CodeFragment.Kind
						    , order : Traversal.Order = Traversal.PREORDER)  { 
		
		val nmBuffer : StringBuilder = new StringBuilder()
		val className : String = node.name
		nmBuffer.append(s"Derive_$className")
		val classNm : String = nmBuffer.toString()
		val classNameFixer = "[-.]+".r
		val classname : String = classNameFixer.replaceAllIn(classNm,"_")
		val scalaDataType = PmmlTypes.scalaDataType(node.dataType)
		val derivedDataTypeNm = PmmlTypes.scalaDerivedDataType(scalaDataType)
	
		clsBuffer.append(s"class $classname (name : String, dataType : String, validValues: ArrayBuffer[(String,String)], leftMargin : String, rightMargin : String, closure : String) \n")
		clsBuffer.append(s"      extends DerivedField(name, dataType, validValues, leftMargin, rightMargin, closure) with LogTrait { \n\n")
		
	}
	
	/** 
	 *  Answer the IfActionElements for this node if it is the top level function in the supplied derived field.
	 */
	def IfActionElementsFromTopLevelChild(node : xDerivedField) :  Option[ArrayBuffer[PmmlExecNode]]= {
		val noChildren : Int = node.Children.length 
		val actionElements = if (noChildren == 1) {
			if (node.Children.apply(0).isInstanceOf[xApply]) {
				val applyFcn : xApply = node.Children.apply(0).asInstanceOf[xApply]			
				val ifActionElements = applyFcn.IfActionElements
				if (ifActionElements.length > 0) {
					Some(ifActionElements) 
				} else {
					None
				}
			} else {
				None
			}
		} else
			None
		actionElements
	}

	def applyFromTopLevelChild(node : xDerivedField) :  Option[xApply] = {
		val noChildren : Int = node.Children.length 
		val Apply = if (noChildren >= 1 && node.Children.apply(0).isInstanceOf[xApply]) {
				Some(node.Children.apply(0).asInstanceOf[xApply])
		} else None
		
		Apply
	}
	
	def quoteStr(dataType : String) : String = {
		if (dataType == "string" || dataType == "date" || dataType == "time" || dataType == "dateTime") {
			s"${'"'}"
		} else {
			""
		}
	  
	}
	
	def derivedFieldExecFcn(clsBuffer : StringBuilder
							, node : xDerivedField
						    , ctx : PmmlContext
						    , generator : PmmlModelGenerator
						    , generate : CodeFragment.Kind
						    , order : Traversal.Order = Traversal.PREORDER) { 	
		val fcnBuffer : StringBuilder = new StringBuilder()
		val scalaDataType : String = PmmlTypes.scalaDataType(node.dataType)
		val derivedDataType : String = PmmlTypes.scalaDerivedDataType(scalaDataType)
  		val returnDataValueType : String = PmmlTypes.scalaTypeToDataValueType(scalaDataType)
		
		val fldNameVal : String = node.name
		val fldNameFixer = "[-.]+".r
		val fldName : String = fldNameFixer.replaceAllIn(fldNameVal,"_")

		clsBuffer.append(s"    override def execute(ctx : Context) : $returnDataValueType = {\n")
		
		if (node.name == "IfElsePred1") {
			val stop : Boolean = true
		}
		
		if (ctx.injectLogging) {
			clsBuffer.append(s"        logger.info(${'"'}Derive${'_'}${node.name} entered...${'"'})\n")
		}
		clsBuffer.append(s"        val $fldName = ")
		val ifActionElems : Option[ArrayBuffer[PmmlExecNode]] = IfActionElementsFromTopLevelChild(node)
		val apply : Option[xApply] = applyFromTopLevelChild(node)
		val fcnName : String = apply match {
			case Some(apply) => {
				val fcnNm : String = if (apply.function == "if") {
					/** grab the predicate for the if and print it... the 'if' actions have been stripped already for top level if functions */
					generator.generateCode1(Some(apply.Children.head), fcnBuffer, generator, CodeFragment.FUNCCALL)
					""
				} else {
					generator.generateCode1(Some(apply), fcnBuffer, generator, CodeFragment.FUNCCALL)
					apply.function
				}
				fcnNm
			}
			case _ => ""
		}
 		clsBuffer.append(fcnBuffer.toString)

		var truthStr : String = ""
		var liesStr : String = ""
		var ifActionElemsLen : Int = 0
		val actionBuffer : StringBuilder = new StringBuilder
		ifActionElems match { 
			case Some(ifActionElems) => {
				if (ifActionElems.length == 2)  {
					val truthAction : PmmlExecNode = ifActionElems.apply(0)
					val falseAction : PmmlExecNode = ifActionElems.apply(1)
					generator.generateCode1(Some(truthAction), actionBuffer, generator, CodeFragment.FUNCCALL)
					truthStr = actionBuffer.toString
					actionBuffer.clear
					generator.generateCode1(Some(falseAction), actionBuffer, generator, CodeFragment.FUNCCALL)
					liesStr = actionBuffer.toString
					ifActionElemsLen = 2
				}
			}
			case _ => None
		}
 		
 		if (ifActionElemsLen == 2) {
			clsBuffer.append(s"\n        var result : $scalaDataType = if ($fldName) { $truthStr } else { $liesStr }\n")
			if (ctx.injectLogging) {
				clsBuffer.append(s"\n        logger.info(s${'"'}Derive${'_'}${node.name} result = ${'$'}${'{'}result.toString${'}'}${'"'})\n")
			}
			clsBuffer.append(s"        ctx.xDict.apply(${'"'}$fldNameVal${'"'}).Value(new $returnDataValueType(result))\n")
	  		clsBuffer.append(s"        new $returnDataValueType(result)\n")
		} else {
			if (ctx.injectLogging) {
				clsBuffer.append(s"\n        logger.info(s${'"'}Derive${'_'}${node.name} result = ${'$'}${'{'}$fldName.toString${'}'}${'"'})\n")
			}
			clsBuffer.append(s"\n        ctx.xDict.apply(${'"'}$fldNameVal${'"'}).Value(new $returnDataValueType($fldName))\n")
			clsBuffer.append(s"        new $returnDataValueType($fldName)\n")
		}
 		
 		clsBuffer.append(s"    }\n")
	}
	
	def derivedFieldFcnHelper(node : xDerivedField
						    , ctx : PmmlContext
						    , generator : PmmlModelGenerator
						    , generate : CodeFragment.Kind
						    , order : Traversal.Order = Traversal.PREORDER) : String = { 
		val clsBuffer : StringBuilder = new StringBuilder()
		
		if (ctx.UpdateClassQueue.size > 0) {
			PmmlError.logError(ctx, "There are items in the update class queue.. evidently they were not dumped. They are:")
			for ( updateClsStr <- ctx.UpdateClassQueue) {
				logger.error(s"\n$updateClsStr")
			}	  
			ctx.UpdateClassQueue.clear
		}

		derivedFieldClassSignature(clsBuffer, node, ctx, generator, generate, order)
		derivedFieldExecFcn(clsBuffer, node, ctx, generator, generate, order)
		
		/** Just after closing the class with the '}' below, check to see if there are any 
		 *  update classes to inject into the stream that service field and container 
		 *  updates explicitly via an xApply function use.
		 *  
		 */
 		clsBuffer.append(s"\n}\n")
		for ( updateClsStr <- ctx.UpdateClassQueue) {
			clsBuffer.append(updateClsStr)
		}
 		clsBuffer.append(s"\n\n")
		ctx.UpdateClassQueue.clear
		
  		
 		clsBuffer.toString()
  			
 	}	


	/**
	 *  class <ApplicationName><Version> (values from the function header) that will have the primary method 
	 *  used by the client to run the model (execute).  The class has no arguments so that the class can be
	 *  easily instantiated by name.  The beneficiary data is supplied to the initialize function that is 
	 *  printed in the modelClassBody function below.
	 *  
	 *  Its body will instantiate the runtime context in place.  The user will instantiate the class
	 *  by name and then call its initialize method and execute as follows:
	 *  	
	 *   	val modelClass = compiler.classInstance("<ModelClassName>", ".")
	 *		val modelInst = modelClass.newInstance
	 *		val methInit=modelClass.getMethod("initialize")
	 *		methInit.invoke(modelInstance)
	 *		val methExec=modelClass.getMethod("execute")
	 *		val modelResults : ArrayBuffer[(String,String,AnyRef)] = methExec.invoke(modelInstance)
	 *  
	 *  	(send the results to the consumer(s))
	 *
	 *	The returned modelResults in the above example are tuple3 instances of (model variable name, model field type, 
	 *  model variable value).  The field type is either "predicted" or "supplementary" ... the only two kinds of 
	 *  values returned for model results.
	 *  
	 *  Long term, the initialize function of the <ModelClassName> instance will perform the filter and aggregation, as
	 *  needed, according to the elements placed in the Transaction Dictionary.  This will produce the "computed" or 
	 *  derived values for some of the transformation dictionary items.  Note that the other transformation dictionary 
	 *  derived fields will only be updated if referenced by a rule or other transformation dictionary derived field..  
	 *  This part of the code is primarily boilerplate.        
	 */
	def modelClassComment(ctx : PmmlContext, generator : PmmlModelGenerator) : String = {
		val commentBuffer : StringBuilder = new StringBuilder()

		/** Get the classname for this object */
		val classname = if (ctx.pmmlTerms.contains("ClassName")) {
				val someNm = ctx.pmmlTerms.apply("ClassName") 
				someNm match {
				  case Some(someNm) => someNm
				  case _ => "NoName"
				}
			} else {
				generateClassName(ctx)
			}

		/** 
		 *  Top of file gets the import for the engine's jar that contains the Beneficiary 
		 *  plus a comment that marks the generated source with identifying information, including 
		 *  the PMML file path that was used to build the source
		 */
		val appName : Option[String] = ctx.pmmlTerms.apply("ApplicationName")
		val fcnName : Option[String] = ctx.pmmlTerms.apply("FunctionName")
		val pmmlPath : Option[String] = ctx.pmmlTerms.apply("PMML")
		val modelVersion : Option[String] = ctx.pmmlTerms.apply("Version")
		val copyrightTxt : Option[String] = ctx.pmmlTerms.apply("Copyright")
		val descriptionTxt : Option[String] = ctx.pmmlTerms.apply("Description")
		val modelName : Option[String] = ctx.pmmlTerms.apply("ModelName")
		
		val nmspc : String = "System" /** only System namespace possible at the moment */
		val optVersion = ctx.pmmlTerms.apply("VersionNumber")
		val versionNo : String = optVersion match {
		  case Some(optVersion) => optVersion.toString
		  case _ => "0000"
		}
		  
		val clientName : String = ctx.ClientName
		val modelPkg = s"com.$clientName.${nmspc}_${classname}_$versionNo.pmml"
		ctx.pmmlTerms("ModelPackageName") = Some(modelPkg)
		commentBuffer.append(s"package $modelPkg\n\n")

		/** Add core udf lib always to import so names don't have to be qualified with full package spec. */
		commentBuffer.append(s"/**Core Udfs... */\n\n")
		commentBuffer.append(s"import com.ligadata.pmml.udfs._\n")
		commentBuffer.append(s"import com.ligadata.pmml.udfs.Udfs._\n")
		/** If there were user defined udfs defined in the model, add these packages as well. */
		val pkgNames : Array[String] = ctx.udfSearchPath
		val pkgsOnly : Array[String] = if (pkgNames.size > 0) {
			if (pkgNames.contains(".")) {
				val pkgs : Array[String] = pkgNames.map(pkg => {
					val pkgNmNodes : Array[String] = pkg.split('.').dropRight(1)
					val buf : StringBuilder = new StringBuilder
					pkgNmNodes.addString(buf, ".")
					buf.toString
				})
				pkgs
			} else {
				Array[String]()
			}
		} else {
			Array[String]()
		}
		if (pkgNames.size > 0) {
			commentBuffer.append(s"/** Custom Udf Libraries Specified in PMML */\n")
			pkgNames.foreach( fullPkgObjName => {
				commentBuffer.append(s"import $fullPkgObjName._\n")
			})
			pkgsOnly.foreach( pkg => {
				commentBuffer.append(s"import $pkg._\n")
			})
		} else {
			commentBuffer.append(s"/** No Custom Udf Libraries Specified in PMML */\n")
		}
		/** Give the rest... */
		commentBuffer.append(s"/** Other Packages... */\n")
		commentBuffer.append(s"import com.ligadata.OnLEPBase._\n")
		commentBuffer.append(s"import com.ligadata.Pmml.Runtime._\n")
		commentBuffer.append(s"import scala.collection.mutable._\n")
		commentBuffer.append(s"import scala.collection.immutable.{ Map }\n")
		commentBuffer.append(s"import scala.collection.immutable.{ Set }\n")
		commentBuffer.append(s"import scala.math._\n")
		commentBuffer.append(s"import scala.collection.immutable.StringLike\n")
		commentBuffer.append(s"import scala.util.control.Breaks._\n")	
		commentBuffer.append(s"\n")
		
		commentBuffer.append(s"/**\n")
		appName match {
		  case Some(appName)      =>    commentBuffer.append(s"    Application Name         : $appName\n")
		  case _ => commentBuffer.append(s"    Application Name            : N/A\n")
		}
		modelVersion match {
		  case Some(modelVersion) =>    commentBuffer.append(s"    PMML Model Version       : $modelVersion\n")
		  case _ => commentBuffer.append(s"    Model Name                  :  N/A\n")
		}
		modelName match {
		  case Some(modelName) =>       commentBuffer.append(s"    Model Name               : $modelName\n")
		  case _ => commentBuffer.append(s"    Model Name                  :  N/A\n")
		}
		fcnName match {
		  case Some(fcnName)   =>       commentBuffer.append(s"    Function Name            : $fcnName\n")
		  case _ => commentBuffer.append(s"    Function Name                  :  N/A\n")
		}
		pmmlPath match {
		  case Some(pmmlPath)  =>       commentBuffer.append(s"    PMML Model Source        : $pmmlPath\n")
		  case _ => commentBuffer.append(s"    PMML Model Source                   :  N/A\n")
		}
		copyrightTxt match {
		  case Some(copyrightTxt)  =>   commentBuffer.append(s"    Copyright                : $copyrightTxt\n")
		  case _ => commentBuffer.append(s"    Copyright                   :  N/A\n")
		}
		descriptionTxt match {
		  case Some(descriptionTxt)  => commentBuffer.append(s"    Description              : $descriptionTxt\n")
		  case _ => commentBuffer.append(s"    Description                   :  N/A\n")
		}
		commentBuffer.append(s"*/\n")
		commentBuffer.append(s"\n")
		
		commentBuffer.toString
 	}

	/** 
	 *  Generate a name for the class based upon the id info found in the Header 
	 *  Squeeze all but typical alphameric characters from app name and version string
	 *  Several side effects.  Collect the ClassName and VersionNumber for ctx.pmmlTerms.
	 */
	def generateClassName(ctx : PmmlContext) : String = {
		val appName : Option[String] = ctx.pmmlTerms.apply("ApplicationName")
		val modelVersion : Option[String] = ctx.pmmlTerms.apply("Version")
		val nmBuffer : StringBuilder = new StringBuilder()
		val numBuffer : StringBuilder = new StringBuilder()
		val alphaNumPattern = "[0-9A-Za-z_]+".r
		val numPattern = "[0-9]+".r
		var classNameString : String = appName match {
			case Some(appName) => appName
			case _ => "NO_CLASSNAME_SUPPLIED_FOR_THIS_MODEL"
		}
		val alphaNumPieces1 = alphaNumPattern.findAllIn(classNameString)

		val versionStr : String = modelVersion match {
		  case Some(optVersion) => optVersion.toString
		  case _ => "there is no class name for this model... incredible as it seems"
		}
		val numVersionPieces = numPattern.findAllIn(versionStr)
		
		
		for (piece <- alphaNumPieces1) nmBuffer.append(piece)

		for (piece <- numVersionPieces) numBuffer.append(piece)
		if (numBuffer.length == 0) {
			numBuffer.append("101")
		}
		/** 
		 *  No longer is the version number appended to the model name... the model name is 
		 *  only the value PMML Application element found in the PMML header prefixed with
		 *  the "System" namespace.  We have no special provision at this time for using an
		 *  alternate namespace for the model.  We may need to change this by allowing the
		 *  namespace to be specified in the PMML model's header.
		 *  
		 *  nmBuffer.append(numBuffer.toString)
		 */
		
		val classname1 : String = nmBuffer.toString
		val classNameFixer = "[-. ]+".r
		val classname : String = classNameFixer.replaceAllIn(classname1,"_")
		
		logger.info(s"Class Name to be created: $classname")
		
		/** Cache the class name in the ctx dictionary of useful terms */
		ctx.pmmlTerms("ClassName") = Some(classname)
		val vnum : Int = numBuffer.toString.toInt
		ctx.pmmlTerms("VersionNumber") = Some(vnum.toString)
		
		classname
	}

	def objBody(ctx : PmmlContext, generator : PmmlModelGenerator) : String = {
		val objBuffer : StringBuilder = new StringBuilder()
	  
		/** Get the classname for this object */
		val classname = if (ctx.pmmlTerms.contains("ClassName")) {
				val someNm = ctx.pmmlTerms.apply("ClassName") 
				someNm match {
				  case Some(someNm) => someNm
				  case _ => "NoName"
				}
			} else {
				generateClassName(ctx)
			}
			
		val optVersion = ctx.pmmlTerms.apply("VersionNumber")
		val versionNo : String = optVersion match {
		  case Some(optVersion) => optVersion.toString
		  case _ => "there is no class name for this model... incredible as it seems"
		}
		val verNoStr : String = "_" + versionNo.toString
		
		val nmspc : String = "System" /** only System namespace possible at the moment */
		if (ctx.injectLogging) {
			objBuffer.append(s"object ${nmspc}_$classname$verNoStr extends ModelBaseObj with LogTrait {\n") 
		} else {
			objBuffer.append(s"object ${nmspc}_$classname$verNoStr extends ModelBaseObj {\n") 
		}
		
		/** generate static variables */
		val somePkg : Option[String] = ctx.pmmlTerms.apply("ModelPackageName")
		val modelName = somePkg match {
		  case Some(somePkg) => s"${'"'}$nmspc.$classname${'"'}"
		  case _ => "None"
		}


		objBuffer.append(s"    def getModelName: String = $modelName\n")
		objBuffer.append(s"    def getVersion: String = ${'"'}$versionNo${'"'}\n")
		objBuffer.append(s"    def getModelVersion: String = getVersion\n")

		val msgs : ArrayBuffer[(String, Boolean, BaseTypeDef, String)] = if (ctx.containersInScope == null || ctx.containersInScope.size == 0) {
			PmmlError.logError(ctx, "No input message(s) specified for this model. Please specify messages variable with one or more message names as values.")
			ArrayBuffer[(String, Boolean, BaseTypeDef, String)]()
		} else {
			/** select any containers in scope that are not = gCtx and have been marked as constructor parameter (container._2._1) */
			ctx.containersInScope.filter( container => { container._1 != "gCtx" && container._2 })	
		}
		
		/** prepare message validation table... the list of message parameters' full class names */
		val validMsgBuffer : StringBuilder = new StringBuilder()
		validMsgBuffer.append(s"val validMessages = Array(")
		var msgCnt : Int = 0
		msgs.foreach( each => { 
			val (_, _, containerTypeDef, _) = each
			val className : String = containerTypeDef.physicalName    //.className
			validMsgBuffer.append(s"${'"'}$className${'"'}")	
			msgCnt += 1
			if (msgCnt < msgs.size) 
				validMsgBuffer.append(", ")
		})
		validMsgBuffer.append(s")")
		val valEvntArrayInstance = validMsgBuffer.toString
		
		/** Add the IsValidMessage function  */
		objBuffer.append(s"    $valEvntArrayInstance\n")   
		objBuffer.append(s"    def IsValidMessage(msg: MessageContainerBase): Boolean = { \n")
		objBuffer.append(s"        validMessages.filter( m => m == msg.getClass.getName).size > 0\n")
		objBuffer.append(s"    }\n")  /** end of IsValidMessage fcn  */		
		objBuffer.append(s"\n")

		/** plan for the day when there are multiple messages present in the constructor */
		val msgNameContainerInfo : Array[(String, Boolean, BaseTypeDef, String)] = ctx.containersInScope.filter( ctnr => {
			val (msgName, isPrintedInCtor, msgdef, varName) : (String, Boolean, BaseTypeDef, String) = ctnr
			isPrintedInCtor
		}).toArray

		/** pick the first one (and only one) AFTER the gCtx for now */
		val msgContainerInfoSize : Int = msgNameContainerInfo.size
		if (msgContainerInfoSize <= 1) {
			logger.error("unable to detect message to work with... there must be one") /** crash this ... with next statement */
		}
		val msgContainer : (String, Boolean, BaseTypeDef, String) = msgNameContainerInfo.tail.head
		val (msgName, isPrintedInCtor, msgTypedef, varName) : (String, Boolean, BaseTypeDef, String) = msgContainer
		val msgTypeStr : String = msgTypedef.typeString
		val msgInvokeStr : String = s"msg.asInstanceOf[$msgTypeStr]"
		
		objBuffer.append(s"    def CreateNewModel(tempTransId: Long, gCtx : EnvContext, msg : MessageContainerBase, tenantId: String): ModelBase =\n")
		objBuffer.append(s"    {\n") 
		objBuffer.append(s"           new ${nmspc}_$classname$verNoStr(gCtx, $msgInvokeStr, getModelName, getVersion, tenantId, tempTransId)\n")
		objBuffer.append(s"    }\n") 	
		objBuffer.append(s"\n")

		objBuffer.append(s"} \n")

		objBuffer.toString
	}

	def modelClassBody(ctx : PmmlContext, generator : PmmlModelGenerator) : String = {
		val clsBuffer : StringBuilder = new StringBuilder()
			
		/** Get the classname for this object */
		val classname = if (ctx.pmmlTerms.contains("ClassName")) {
				val someNm = ctx.pmmlTerms.apply("ClassName") 
				someNm match {
				  case Some(someNm) => someNm
				  case _ => "NoName"
				}
			} else {
				generateClassName(ctx)
			}
		
		/** 
		 *  Add the class declaration to the the class body.  Spin thru the ctx's messageTypes to find those
		 *  containers that are "messages" and to be part of the formal signature.
		 */
		
		val ctorMsgsBuffer : StringBuilder = new StringBuilder()
		ctx.containersInScope.foreach( ctnr => {
			val (msgName, isPrintedInCtor, msgdef, varName) : (String, Boolean, BaseTypeDef, String) = ctnr
			if (isPrintedInCtor) {
				if (ctorMsgsBuffer.length > 0) {
					ctorMsgsBuffer.append(", ")
				}
				ctorMsgsBuffer.append("val ")
				ctorMsgsBuffer.append(varName)
				ctorMsgsBuffer.append(" : " )
				ctorMsgsBuffer.append(msgdef.typeString)
			}
		})
		val ctorGtxAndMessagesStr : String = ctorMsgsBuffer.toString

		val optVersion = ctx.pmmlTerms.apply("VersionNumber")
		val versionNo : String = optVersion match {
		  case Some(optVersion) => optVersion.toString
		  case _ => "there is no class name for this model... incredible as it seems"
		}
		val verNoStr : String = "_" + versionNo.toString

		val nmspc : String = "System_" /** only System namespace possible at the moment */
		clsBuffer.append(s"class $nmspc$classname$verNoStr($ctorGtxAndMessagesStr, val modelName:String, val modelVersion:String, val tenantId: String, val tempTransId: Long)\n")
		if (ctx.injectLogging) {
			clsBuffer.append(s"   extends ModelBase with LogTrait {\n") 
		} else {
			clsBuffer.append(s"   extends ModelBase {\n") 
		}
		clsBuffer.append(s"    val ctx : com.ligadata.Pmml.Runtime.Context = new com.ligadata.Pmml.Runtime.Context(tempTransId)\n")
		clsBuffer.append(s"    def GetContext : Context = { ctx }\n")
		
		clsBuffer.append(s"    override def getModelName : String = $nmspc$classname$verNoStr.getModelName\n")
		clsBuffer.append(s"    override def getVersion : String = $nmspc$classname$verNoStr.getVersion\n")
		clsBuffer.append(s"    override def getTenantId : String = tenantId\n")
		clsBuffer.append(s"    override def getTempTransId: Long = tempTransId\n")
		
		clsBuffer.append(s"    var bInitialized : Boolean = false\n")
		clsBuffer.append(s"    var ruleSetModel : RuleSetModel = null\n")
		clsBuffer.append(s"    var simpleRules : ArrayBuffer[SimpleRule] = new ArrayBuffer[SimpleRule]\n")
		
		clsBuffer.append(s"\n")
		clsBuffer.append(s"    /** Initialize the data and transformation dictionaries */\n")
		clsBuffer.append(s"    if (! bInitialized) {\n")
		clsBuffer.append(s"         initialize\n")
		clsBuffer.append(s"         bInitialized = true\n")
		clsBuffer.append(s"    }\n")
		clsBuffer.append(s"\n")		
		
		/** plan for the day when there are multiple messages present in the constructor */
		val msgNameContainerInfo : Array[(String, Boolean, BaseTypeDef, String)] = ctx.containersInScope.filter( ctnr => {
			val (msgName, isPrintedInCtor, msgdef, varName) : (String, Boolean, BaseTypeDef, String) = ctnr
			isPrintedInCtor
		}).toArray

		/** pick the first one (and only one) AFTER the gCtx for now */
		val msgContainerInfoSize : Int = msgNameContainerInfo.size
		if (msgContainerInfoSize <= 1) {
			logger.error("unable to detect message to work with... there must be one") /** crash this ... with next statement */
		}
		val msgContainer : (String, Boolean, BaseTypeDef, String) = msgNameContainerInfo.tail.head
		val (msgName, isPrintedInCtor, msgTypedef, varName) : (String, Boolean, BaseTypeDef, String) = msgContainer

		clsBuffer.append(s"\n")
		clsBuffer.append(s"    /***********************************************************************/\n")
		clsBuffer.append(s"    ctx.dDict.apply(${'"'}gCtx${'"'}).Value(new AnyDataValue(gCtx))\n")
		clsBuffer.append(s"    ctx.dDict.apply(${'"'}$msgName${'"'}).Value(new AnyDataValue($msgName))\n")
		clsBuffer.append(s"    ctx.dDict.apply(${'"'}msg${'"'}).Value(new AnyDataValue(msg))\n")
		clsBuffer.append(s"    /***********************************************************************/\n")
		/** 
		 *  Add the initialize function to the the class body 
		 */
		clsBuffer.append(s"    def initialize : $nmspc$classname$verNoStr = {\n")
		clsBuffer.append(s"\n")
		
		val ruleCtors = ctx.RuleRuleSetInstantiators.apply("SimpleRule")
		val ruleCtorsInOrder = ctx.simpleRuleInsertionOrder
		val ruleSetModel = ctx.RuleRuleSetInstantiators("RuleSetModel")
		val ruleSetModelCtorStr : String = ruleSetModel.head  // there is only one of these 
		
		/** initialize the RuleSetModel and SimpleRules array with new instances of respective classes */
		clsBuffer.append(s"        ctx.SetRuleSetModel($ruleSetModelCtorStr)\n")
		clsBuffer.append(s"        val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel\n")
		clsBuffer.append(s"        /** Initialize the RuleSetModel and SimpleRules array with new instances of respective classes */\n")		
		clsBuffer.append(s"        var simpleRuleInstances : ArrayBuffer[SimpleRule] = new ArrayBuffer[SimpleRule]()\n")
		//ruleCtors.foreach( ruleCtorStr => clsBuffer.append(s"        ruleSetModel.AddRule($ruleCtorStr)\n"))
		ruleCtorsInOrder.foreach( ruleCtorStr => clsBuffer.append(s"        ruleSetModel.AddRule($ruleCtorStr)\n"))
		
		clsBuffer.append(s"        /* Update the ruleset model with the default score and rule selection methods collected for it */\n")
		val dfltScore : String = ctx.DefaultScore
		clsBuffer.append(s"        ruleSetModel.DefaultScore(new StringDataValue(${'"'}$dfltScore${'"'}))\n")
		ctx.RuleSetSelectionMethods.foreach( selMethod => {
				val criterion : String = selMethod.criterion
		  		clsBuffer.append(s"        ruleSetModel.AddRuleSelectionMethod(new RuleSelectionMethod(${'"'}$criterion${'"'}))\n")
		 })
		
		clsBuffer.append(s"\n        /* Update each rules ScoreDistribution if necessary.... */\n")
		var i : Int = 1
		if (ctx.RuleScoreDistributions.size > 0) {
			for(rsds  <- ctx.RuleScoreDistributions) {
				if (rsds.size > 0) {
					clsBuffer.append(s"        val rule$i : SimpleRule = ruleSetModel.RuleSet().apply($i)\n")
					for (rsd <- rsds) {
						val value = rsd.value
						val recordCount = rsd.recordCount
						val confidence = rsd.confidence
						val probability = rsd.probability
						clsBuffer.append(s"            rule$i.addScoreDistribution(new ScoreDistribution(${'"'}$value${'"'}, $recordCount, $confidence, $probability))\n")
					}
				} else {
					clsBuffer.append(s"        /** no rule score distribution for rule$i */\n")
				}
				i = i + 1
			}
		} else {
			clsBuffer.append(s"        /* ... no score distribution information present in the pmml */\n")
		}
		
 		/** Add the mining schema to the ruleSetModel object */
		clsBuffer.append(s"\n        /* Update each ruleSetModel's mining schema dict */\n")
  		ctx.miningSchemaInstantiators.foreach(fld => { 
  			val mFieldName : String = fld._1
  			val ctor : String = fld._2
  			clsBuffer.append(s"        ruleSetModel.AddMiningField(${'"'}$mFieldName${'"'}, $ctor)\n")
  		})

		/** ... and for convenience update the ctx with them  */
		clsBuffer.append(s"\n")
		clsBuffer.append(s"        /* For convenience put the mining schema map in the context as well as ruleSetModel */\n")
		clsBuffer.append(s"        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())\n")
		
		/** 
		 *  Several additional pieces of information are associated with the RuleSetModel and SimpleRule classes.
		 *  Add code to decorate the instances of these classes created in the snippet above. 
		 */
		
		/** initialize the data dictionary */
		clsBuffer.append(s"        /** initialize the data dictionary */\n")
		val dictBuffer : StringBuilder = new StringBuilder()
		val ddNode : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("DataDictionary") 
		ddNode match {
		  case Some(ddNode) => generator.generateCode1(Some(ddNode), dictBuffer, generator, CodeFragment.VALDECL)
		  case _ => PmmlError.logError(ctx, s"there was no data dictionary avaialble... whoops!\n")
		}
		clsBuffer.append(dictBuffer.toString)
		clsBuffer.append(s"\n")
		
		/** initialize the transformation dictionary (derived field part) */
		clsBuffer.append(s"        /** initialize the transformation dictionary (derived field part) */\n")
		val xNode : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("TransformationDictionary") 
		dictBuffer.clear
		xNode match {
		  case Some(xNode) => generator.generateCode1(Some(xNode), dictBuffer, generator, CodeFragment.VALDECL)
		  case _ => PmmlError.logError(ctx, s"there was no data dictionary avaialble... whoops!")
		}		
		clsBuffer.append(dictBuffer.toString)
		clsBuffer.append(s"\n")
		
		/** fill the Context's mining field dictionary ... generator generates ruleSet.addMiningField(... */
		clsBuffer.append(s"        /** fill the Context's mining field dictionary ...*/\n")
		val rsmNode : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("RuleSetModel") 
		dictBuffer.clear
		clsBuffer.append(s"        //val ruleSetModel : RuleSetModel = ctx.GetRuleSetModel\n")
		rsmNode match {
		  case Some(rsmNode) => generator.generateCode1(Some(rsmNode), dictBuffer, generator, CodeFragment.MININGFIELD)
		  case _ => PmmlError.logError(ctx, s"no mining fields... whoops!\n")
		}
		clsBuffer.append(dictBuffer.toString)
		clsBuffer.append(s"        /** put a reference of the mining schema map in the context for convenience. */\n")
		clsBuffer.append(s"        ctx.MiningSchemaMap(ruleSetModel.MiningSchemaMap())\n")
		clsBuffer.append(s"\n")
		
		clsBuffer.append(s"        /** Build the dictionary of model identifiers \n")
		clsBuffer.append(s"            Keys are: \n")
		clsBuffer.append(s"                 ApplicationName , FunctionName, PMML, Version,  \n")
		clsBuffer.append(s"                 Copyright, Description, ModelName, ClassName \n")
		clsBuffer.append(s"         */\n")
		var modelIdent : Option[String] = ctx.pmmlTerms.apply("ApplicationName")
		val modelId = modelIdent match {
		  case Some(modelIdent) => s"${'"'}$modelIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}ApplicationName${'"'}) = Some($modelId)\n")
		
		var fcnIdent : Option[String] = ctx.pmmlTerms.apply("FunctionName")
		val fcnId = fcnIdent match {
		  case Some(fcnIdent) => s"${'"'}$fcnIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}FunctionName${'"'}) = Some($fcnId)\n")
		
		var srcIdent : Option[String] = ctx.pmmlTerms.apply("PMML")
		val srcId = srcIdent match {
		  case Some(srcIdent) => s"${'"'}$srcIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}PMML${'"'}) = Some($srcId)\n")
		
		var verIdent : Option[String] = ctx.pmmlTerms.apply("Version")
		val verId = verIdent match {
		  case Some(verIdent) => s"${'"'}$verIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}Version${'"'}) = Some($verId)\n")
		
		var cpyIdent : Option[String] = ctx.pmmlTerms.apply("Copyright")
		val cpyId = cpyIdent match {
		  case Some(cpyIdent) => s"${'"'}$cpyIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}Copyright${'"'}) = Some($cpyId)\n")
		
		var descrIdent : Option[String] = ctx.pmmlTerms.apply("Description")
		val descrId = descrIdent match {
		  case Some(descrIdent) => s"${'"'}$descrIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}Description${'"'}) = Some($descrId)\n")
		
		var mdlIdent : Option[String] = ctx.pmmlTerms.apply("ModelName")
		val mdlId = mdlIdent match {
		  case Some(mdlIdent) => s"${'"'}$mdlIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}ModelName${'"'}) = Some($mdlId)\n")
		clsBuffer.append(s"\n")
		
		var clsIdent : Option[String] = ctx.pmmlTerms.apply("ClassName")
		val clsId = clsIdent match {
		  case Some(clsIdent) => s"${'"'}$clsIdent${'"'}"
		  case _ => "None"
		}
		clsBuffer.append(s"        ctx.pmmlModelIdentifiers(${'"'}ClassName${'"'}) = Some($clsId)\n")
		clsBuffer.append(s"\n")
		clsBuffer.append(s"        this\n")
		clsBuffer.append(s"    }   /** end of initialize fcn  */	\n")  /** end of initialize fcn  */		
		clsBuffer.append(s"\n")
		
   		/** 
		 *  Add the execute function to the the class body... the prepareResults function will build the return array for consumption by engine. 
		 */
		clsBuffer.append(s"    /** provide access to the ruleset model's execute function */\n")
		clsBuffer.append(s"    def execute(emitAllResults : Boolean) : ModelResult = {\n")
		clsBuffer.append(s"        ctx.GetRuleSetModel.execute(ctx)\n")
		clsBuffer.append(s"        prepareResults(emitAllResults)\n")
		clsBuffer.append(s"    }\n")
		clsBuffer.append(s"\n")
		
		preparePrepareResultsFunction(ctx, classname, generator, clsBuffer)
		clsBuffer.toString
 	}

	/**
  		This function will add the "prepareResults" function to the model class being written.
  		It will take the mining variables and prepare the following objects... and array of 
  		
			class Result(var resultType: MiningVarInfo, var result: Any) {
			}
			
		from the mining prediction and supplementary variables and a statement to instantiate one 
		of these:
		
			class ModelResult(var eventDate: Long
				, var executedTime: Long
				, var mdlName: String
				, var mdlVersion: String
				, results: Array[Result]) {
      		}
  	 */
	
	def preparePrepareResultsFunction(ctx : PmmlContext
									, classname : String
									, generator : PmmlModelGenerator
									, clsBuffer : StringBuilder) : Unit = {
		val prepResultBuffer : StringBuilder = new StringBuilder
		
		prepResultBuffer.append(s"\n")
		prepResultBuffer.append(s"    /** prepare output results scored by the rules. */\n")
		prepResultBuffer.append(s"    def prepareResults(emitAllResults : Boolean) : ModelResult = {\n")
		prepResultBuffer.append(s"\n")
		
		/** NOTE: The mining field values need to be duplicated here so as to not foul the "retain" in the next step... this mining map is a variable
		 *  that actually discards the mining fields that are NOT 'predicted' or 'supplementary' */
		prepResultBuffer.append(s"        val defaultScore : String = GetContext.GetRuleSetModel.DefaultScore().Value\n")
        prepResultBuffer.append(s"        val miningVars : Array[MiningField] = GetContext.GetRuleSetModel.MiningSchemaMap().values.toArray\n")
        prepResultBuffer.append(s"        val predictionFld : MiningField = miningVars.filter(m => m.usageType == ${'"'}predicted${'"'}).head\n") 

		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"        /** If supplied flag is true, emit all results, else base decision on whether prediction*/\n")                       
		prepResultBuffer.append(s"        /** is a value other than the defaultScore.*/\n")                       
		prepResultBuffer.append(s"        val modelProducedResult : Boolean = if (emitAllResults) true else {\n")
		prepResultBuffer.append(s"            val somePrediction : DataValue = ctx.valueFor(predictionFld.name) \n")
		prepResultBuffer.append(s"            val predictedValue : Any = somePrediction match { \n")
		prepResultBuffer.append(s"    	  		     case d    : DoubleDataValue   => somePrediction.asInstanceOf[DoubleDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case f    : FloatDataValue    => somePrediction.asInstanceOf[FloatDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case l    : LongDataValue     => somePrediction.asInstanceOf[LongDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case i    : IntDataValue      => somePrediction.asInstanceOf[IntDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case b    : BooleanDataValue  => somePrediction.asInstanceOf[BooleanDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case ddv  : DateDataValue     => somePrediction.asInstanceOf[DateDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case dtdv : DateTimeDataValue => somePrediction.asInstanceOf[DateTimeDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case tdv  : TimeDataValue     => somePrediction.asInstanceOf[TimeDataValue].Value \n")
		prepResultBuffer.append(s"    	  		     case s    : StringDataValue   => somePrediction.asInstanceOf[StringDataValue].Value \n")
		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"    	  		     case _ => somePrediction.asInstanceOf[AnyDataValue].Value \n")
		prepResultBuffer.append(s"            } \n")
		prepResultBuffer.append(s"            (predictedValue.toString != defaultScore)\n")
		prepResultBuffer.append(s"        }\n")                       
		
		prepResultBuffer.append(s"\n")                       
		
		prepResultBuffer.append(s"        val modelResult : ModelResult = if (modelProducedResult) {\n")                       

		prepResultBuffer.append(s"            val results : Array[Result] = GetContext.GetRuleSetModel.MiningSchemaMap().retain((k,v) => \n")
		prepResultBuffer.append(s"    	  		    v.usageType == ${'"'}predicted${'"'} || v.usageType == ${'"'}supplementary${'"'}).values.toArray.map(mCol => \n")
		prepResultBuffer.append(s"    	  		  	{ \n")
		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"    	  		  	    val someValue : DataValue = ctx.valueFor(mCol.name) \n")
		prepResultBuffer.append(s"    	  		  	    val value : Any = someValue match { \n")
		prepResultBuffer.append(s"    	  		  	        case d    : DoubleDataValue   => someValue.asInstanceOf[DoubleDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case f    : FloatDataValue    => someValue.asInstanceOf[FloatDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case l    : LongDataValue     => someValue.asInstanceOf[LongDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case i    : IntDataValue      => someValue.asInstanceOf[IntDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case b    : BooleanDataValue  => someValue.asInstanceOf[BooleanDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case ddv  : DateDataValue     => someValue.asInstanceOf[DateDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case dtdv : DateTimeDataValue => someValue.asInstanceOf[DateTimeDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case tdv  : TimeDataValue     => someValue.asInstanceOf[TimeDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	        case s    : StringDataValue   => someValue.asInstanceOf[StringDataValue].Value \n")
		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"    	  		  	        case _ => someValue.asInstanceOf[AnyDataValue].Value \n")
		prepResultBuffer.append(s"    	  		  	    } \n")
		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"    	  		  	    new Result(mCol.name, MinVarType.StrToMinVarType(mCol.usageType), value)  \n")
		prepResultBuffer.append(s"\n")                       
		prepResultBuffer.append(s"    	  		  	}) \n")
		prepResultBuffer.append(s"            val millisecsSinceMidnight: Long = dateMilliSecondsSinceMidnight().toLong \n")
		prepResultBuffer.append(s"            val now: org.joda.time.DateTime = new org.joda.time.DateTime() \n")
		prepResultBuffer.append(s"            val nowStr: String = now.toString \n")
		prepResultBuffer.append(s"            val dateMillis : Long = now.getMillis.toLong - millisecsSinceMidnight \n")

		val nmspc : String = "System_" /** only System namespace possible at the moment */
		val optVersion = ctx.pmmlTerms.apply("VersionNumber")
		val versionNo : String = optVersion match {
		  case Some(optVersion) => optVersion.toString
		  case _ => "there is no class name for this model... incredible as it seems"
		}
		val verNoStr : String = "_" + versionNo.toString

		prepResultBuffer.append(s"            new ModelResult(dateMillis, nowStr, $nmspc$classname$verNoStr.getModelName, $nmspc$classname$verNoStr.getModelVersion, results) \n")
		prepResultBuffer.append(s"        } else { null }\n")
		prepResultBuffer.append(s"\n")
		prepResultBuffer.append(s"        modelResult\n")
		prepResultBuffer.append(s"    }\n")
		prepResultBuffer.append(s"\n")
		
		clsBuffer.append(prepResultBuffer.toString)
	}
	
	
	
}




