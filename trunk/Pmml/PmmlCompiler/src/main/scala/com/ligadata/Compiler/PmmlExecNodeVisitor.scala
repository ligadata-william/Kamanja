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
			  val fieldTypeInfo : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(fldRef.field, expandCompoundFieldTypes)
			  if (fieldTypeInfo != null) {
				  fieldTypeInfo.foreach(triple => {
					  val (typeStr, isContainer, typedef) : (String,Boolean,BaseTypeDef) = triple
					  val isAnyKindOfContainer : Boolean = (typedef != null && typedef.isInstanceOf[ContainerTypeDef])
					  val isStructOrMappedType : Boolean = ctx.mdHelper.isContainerWithFieldOrKeyNames(typedef)
					  if (isStructOrMappedType) {
						  val key = fldRef.field.toLowerCase + "." + typeStr
						  ctx.modelInputs(key) = (fldRef.field, typeStr, typedef.NameSpace, typedef.Name, false, null)
					  } else {
						  val containerOrMessageMemberTypes : Array[BaseTypeDef] = ctx.mdHelper.collectMemberContainerTypes(typedef)
						  if (containerOrMessageMemberTypes.size > 0) {
							  containerOrMessageMemberTypes.foreach(typ => {
								  val key = fldRef.field.toLowerCase + "." + typ.typeString
								  ctx.modelInputs(key) = (fldRef.field, typ.typeString, typ.NameSpace, typ.Name, false, null)
							  })
							  
						  }
					  }
				    
				  })
			    
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

/** Derived fields in the transformation dictionary with 'if' functions are processed here.
 *  The if statement's true and false actions are removed from the child list for the node and placed
 *  in the apply (if) function's that contains categorized value array.  
 *  
 *  These actions are rendered after the 'if' predicate is computed.  NOTE: Only the 'top' level
 *  applies are treated this way.  If there are if's nested at lower levels in a nested set of apply elements,
 *  they will be rendered in place.  That sort of expression is untested at this point, but we need to support it.
 */

class IfActionTransform (ctx : PmmlContext) extends PmmlExecVisitor {
  
	override def Visit(node : PmmlExecNode) {
		node match {
		  case x : xDerivedField => {
			  if (IfActionPresent(node)) {
				  stripIfActionsFromApplysChildren(node)
			  }
		  }
		  case _ => None
		}
	}
	
	def IfActionPresent(node : PmmlExecNode) : Boolean = {
 
		val thisIsDerivedField = if (node.isInstanceOf[xDerivedField]) true else false
		var ifActionExprCnt = 0
		if (thisIsDerivedField) {
			val derivedFld : xDerivedField = node.asInstanceOf[xDerivedField]
			
			val thisDerivedFieldHasApply : Boolean = if (derivedFld.Children.length == 1 && 
			    								derivedFld.Children.head.isInstanceOf[xApply]) 
															{ true } else { false }
			ifActionExprCnt = if (thisDerivedFieldHasApply) {
				val topLevelApply : xApply = derivedFld.Children.head.asInstanceOf[xApply]
				val childCnt : Int = topLevelApply.Children.length
				/** 
				 *  If the top level function is one of our udfs and could have numerous arguments... 
				 *  make sure they are preserved. 
				 */
				val fcn : String = topLevelApply.function
				val (minParmsNeeded, maxParmsNeeded) : (Int,Int) = ctx.MetadataHelper.minMaxArgsForTheseFcns(fcn)
				
				if (fcn == "if" && (childCnt - maxParmsNeeded) >= 2) { 
					
					ctx.logger.debug(s"categorizedReturnValuesPresent() - topLevelApply 'if' fcn = $fcn")
					
					2
				} else {
					0
				}
			} else {
				0
			}
		}
		if (ifActionExprCnt > 0) true else false
	}
	
	/** 
	 *  Strip the if action elements from the supplied 'if' apply node leaving just the predicate, reserving them 
	 *  in the apply's IfAction array.
	 *  
	 *  pre-condition : IfActionPresent(node) == true
	 */
	private def stripIfActionsFromApplysChildren(node : PmmlExecNode) : Unit = {
		val topLevelApply : xApply = node.Children.head.asInstanceOf[xApply]
		val n : Int = topLevelApply.Children.length - 2
		val (children, catValues) = topLevelApply.Children.splitAt(n)
		catValues.foreach((value) => {
			val aNode : PmmlExecNode = value.asInstanceOf[PmmlExecNode]
			topLevelApply.addIfAction(aNode)
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


