package com.ligadata.Compiler

import org.apache.log4j.Logger
import com.ligadata.olep.metadata._


trait PmmlExecVisitor {
	
	def Visit(node : PmmlExecNode)
	
}

/** 
 *  										[(message name, (appears in ctor signature?, message type for named message, varName))] 
 *  var containersInScope : ArrayBuffer[(String, Boolean, BaseTypeDef, String)] = ArrayBuffer[(String, Boolean, BaseTypeDef, String)]()
 *  
 *  (model var namespace (could be msg or concept or modelname), variable name, type namespace
   *  					, type name, and whether the variable is global (a concept that has been independently cataloged))
 *  
 */


class ContainerFieldRefCollector(ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		val expandCompoundFieldTypes : Boolean = true
		node match {
		  case x : xFieldRef => {
			  val fldRef : xFieldRef = node.asInstanceOf[xFieldRef]
			  if (fldRef.field.contains('.')) {
				  val names : Array[String] = fldRef.field.split('.')
				  /** FIXME: there could be multiple nodes here */
				  val containerName : String = names(0)
				  val fieldName : String = names(1)
				  val aContainerInScope = ctx.containersInScope.filter(_._1 == containerName)
				  if (aContainerInScope.size > 0) {
					  val containerInfo = aContainerInScope.head
					  val (cName, inCtor, baseType, varName) : (String, Boolean, BaseTypeDef, String) = containerInfo
					  val (typeStr, isCntr, typedef) : (String,Boolean,BaseTypeDef) = ctx.getFieldType(fldRef.field, ! expandCompoundFieldTypes).head
					  val isGlobal : Boolean = (ctx.mgr.Attribute(containerName, fieldName, -1, true) != None)
					  ctx.modelInputs(fldRef.field.toLowerCase()) = (cName, fieldName, baseType.NameSpace, baseType.Name, isGlobal)
				  }
			  }
		  }
		  case _ => None
		}
	}
}


class UdfCollector(ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		node match {
		  case x : xDefineFunction => {
			  ctx.UdfMap(x.name) = x
		  }
		  case _ => None
		}
	}
}



class RuleSetModelCollector(ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		node match {
		  case x : xRuleSetModel => {
			  val srs : xRuleSetModel = x.asInstanceOf[xRuleSetModel]
			  ctx.DefaultScore(srs.DefaultScore)
			  ctx.RuleSetSelectionMethods(srs.ruleSetSelectionMethods)
		  }
		  case _ => None
		}
	}
}

class SimpleRuleCollector(ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		node match {
		  case x : xSimpleRule => {
			  val sr : xSimpleRule = x.asInstanceOf[xSimpleRule]
			  ctx.RuleScoreDistributions(sr.scoreDistributions)
		  }
		  case _ => None
		}
	}
}

/** Derived fields in the transformation dictionary can have a top level function that contains categorized return
 *  values.  These are quite common with the various MapValue types.  It can also be seen with the boolean functions, 
 *  especially used with if in the examples one sees on the web examples.  Virtually any of them could be used
 *  this way, however (e.g., greaterOrEqual, etc). 
 *  
 *  This class strips the categorized values from the top level apply functions and adds them to its 
 *  categorizedValues array.  That is, these categorized values are removed from the Children of the apply node
 *  and moved to the array of categorized values.  This simplifies the code generation later.
 *  
 *  For now, the focus is on the boolean functions whose derived (parent) field has an optype of 'categorical'
 *  The last two xConstants are removed from the Child list of the top level apply.
 */
class CategorizedReturnValueTransform (ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		node match {
		  case x : xDerivedField => {
			  if (categorizedReturnValuesPresent(node)) {
				  stripCategoryConstantsFromApplysChildren(node)
			  }
		  }
		  case _ => None
		}
	}
	
	def categorizedReturnValuesPresent(node : PmmlExecNode) : Boolean = {
 
		val thisIsDerivedField = if (node.isInstanceOf[xDerivedField]) true else false
		var categoryConstCount = 0
		if (thisIsDerivedField) {
			val derivedFld : xDerivedField = node.asInstanceOf[xDerivedField]
			
			val thisDerivedFieldHasApply : Boolean = if (derivedFld.Children.length == 1 && 
			    								derivedFld.Children.head.isInstanceOf[xApply]) 
															{ true } else { false }
			val isCategoricalType : Boolean = if (derivedFld.optype == "categorical" && thisDerivedFieldHasApply) true else false
			categoryConstCount = if (isCategoricalType) {
				val topLevelApply : xApply = derivedFld.Children.head.asInstanceOf[xApply]
				val numConsts = topLevelApply.Children.filter( _.isInstanceOf[xConstant]).length
				val childCnt : Int = topLevelApply.Children.length
				/** 
				 *  If the top level function is one of our udfs and could have numerous constants as arguments 
				 *  make sure they are preserved. 
				 */
				val fcn : String = topLevelApply.function
				val (minParmsNeeded, maxParmsNeeded) : (Int,Int) = ctx.MetadataHelper.minMaxArgsForTheseFcns(fcn)
				
				ctx.logger.debug(s"categorizedReturnValuesPresent() - topLevelApply fcn = $fcn")
				if (fcn == "if" && numConsts >= 2 && (childCnt - maxParmsNeeded) >= 2) { 
					/** verify there at least the last two are constants */
					
					var cnt : Int = 0
					//topLevelApply.Children.foreach((child) => {
					//	val rep : String = child.asString
					//	ctx.logger.debug(s"child($cnt) = $rep")
					//	cnt += 1
				  	//}) 
					val lastTwo : Int = if (topLevelApply.Children.apply(childCnt - 1).isInstanceOf[xConstant] && 
											topLevelApply.Children.apply(childCnt - 2).isInstanceOf[xConstant]) {
						2
					} else {
						0
					} 
					lastTwo 
				} else {
					0
				}
			} else {
				0
			}
		}
		if (categoryConstCount > 0) true else false
	}
	
	/** 
	 *  pre-condition : categorizedReturnValuesPresent(node) == true
	 *  
	 *  call it without this pre-condition in mind and see what happens...
	 */
	def stripCategoryConstantsFromApplysChildren(node : PmmlExecNode) {
		val topLevelApply : xApply = node.Children.head.asInstanceOf[xApply]
		val n : Int = topLevelApply.Children.length - 2
		val (children, catValues) = topLevelApply.Children.splitAt(n)
		catValues.foreach((value) => {
				val aXConst : xConstant = value.asInstanceOf[xConstant]
				topLevelApply.addCategorizedValue(aXConst)
		  	}) 
		topLevelApply.replaceChildren(children)
	}

}

object PmmlExecNodeVisitor {  

	def Visit (rootNode : Option[PmmlExecNode], visitor : PmmlExecVisitor) {
	  
		rootNode match {
		  case Some(rootNode) => rootNode.Visit(visitor)
		  case _ => None
		}
	}
}


