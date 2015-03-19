package com.ligadata.Compiler

import scala.collection.mutable.ArrayBuffer
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._


trait PmmlExecVisitor {
	
	def Visit(node : PmmlExecNode)
	
}

/** 
 *  Collect model input variables from both the incoming messages and derived variables produced by other models.  These
 *  both share the characteristic that they have a compound '.' delimited name.
 */
class ContainerFieldRefCollector(ctx : PmmlContext) extends PmmlExecVisitor with LogTrait {
  
	override def Visit(node : PmmlExecNode) {
		val expandCompoundFieldTypes : Boolean = true
		node match {
		  case x : xFieldRef => {
			  val fldRef : xFieldRef = node.asInstanceOf[xFieldRef]
			  /** Consider multiple '.' delimited name nodes here */
			  if (fldRef.field.contains('.')) {
				  val names : Array[String] = fldRef.field.split('.')

				  /** Consider if this is a model field reference */
				  val hasModelPrefix : Boolean = ctx.MetadataHelper.IsDerivedConcept(fldRef.field)
				  val hasNamespacePrefix : Boolean = ctx.MetadataHelper.HasNameSpacePrefix(fldRef.field)
				  val hasNamespaceButIsNotModel : Boolean = (hasNamespacePrefix && ! hasModelPrefix)

				  val (containerName,fieldName) : (String,String) = if (! hasModelPrefix) {
					  if (hasNamespacePrefix && names.size > 2) (names(1),names(2)) else (names(0),names(1))
				  } else {
					  (null,null)
				  }
				  
				  val aContainerInScope = if (containerName != null) {
					  ctx.containersInScope.filter(_._1 == containerName)  
				  } else {
					  ArrayBuffer[(String, Boolean, BaseTypeDef, String)]()
				  }
 
				  if (aContainerInScope.size > 0) { /** examine containers whose origin is not another model ( ! derived fields) ... */
					  val containerInfo = aContainerInScope.head
					  val (cName, inCtor, baseType, varName) : (String, Boolean, BaseTypeDef, String) = containerInfo
					  val (typeStr, isCntr, typedef) : (String,Boolean,BaseTypeDef) = ctx.getFieldType(fldRef.field, ! expandCompoundFieldTypes).head
					  val isGlobal : Boolean = (ctx.mgr.Attribute(containerName, fieldName, -1, true) != None)

					  /** 
					   *  For the input variables, the type hierarchy from the outer container to the leaf is needed.  As such
					   *  the names (conventional '.' delimited compound name) corresponds to an array of namespace,typename pairs
					   *  found by the getFieldType. 
					   */
					  val typeInfo : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(fldRef.field, expandCompoundFieldTypes)
					  if (typeInfo == null || (typeInfo != null && typeInfo.size == 0) || (typeInfo != null && typeInfo.size > 0 && typeInfo(0)._3 == null)) {
						  //throw new RuntimeException(s"collectModelOutputVars: the mining field $name does not refer to a valid field in one of the dictionaries")
						  logger.error(s"mining field named '${fldRef.field}' does not exist... your model is going to fail... however, let's see how far we can go to find any other issues...")
						  val bogusInput :  ModelInputVariable = new ModelInputVariable(fldRef.field, null, false)
						  ctx.modelInputs(fldRef.field.toLowerCase()) = bogusInput
					  } else {
			
						  val miningTypeInfo : Array[(String, String)] = typeInfo.map( info => {
							  val (typestr, isCntnr, typedef) : (String, Boolean, BaseTypeDef) = info
							  (typedef.NameSpace, typedef.Name)
						  })
						  
						  /** legit (non derived concept) data or field... */
						  val inputVar :  ModelInputVariable = new ModelInputVariable(fldRef.field, miningTypeInfo, hasModelPrefix)
						  ctx.modelInputs(fldRef.field.toLowerCase()) = inputVar		
					  }
				  } else {  /** Is this a derived field?  (i.e., an output variable computed in another model?) */
				    
					  val inputVar :  ModelInputVariable = if (hasModelPrefix) {
						  val (model, modelContainerKey, conceptKey, subFldKeys) : (ModelDef, String, String, String) = ctx.MetadataHelper.GetDerivedConceptModel(fldRef.field)
						  val modelName : String = model.FullNameWithVer
						  val conceptName : String = fldRef.field.split('.').last
						  val inputVarName : String = modelName + "." + conceptName
						  
						  val (typeNameSpace,typeName) : (String,String) = model.OutputVarType(inputVarName)
						  /** Check to see if this type is available in the metadata */
						  val typedef : BaseTypeDef = if (typeNameSpace != null && typeName != null) {
							  val typ : BaseTypeDef = ctx.mgr.ActiveType(typeNameSpace,typeName)
							  if (typ != null) {
								  typ
							  } else {
								  logger.error(s"type could not be found for output variable '${typeNameSpace}.${typeName} in the metadata cache...")
								  null
							  }
						  } else {
							  logger.error(s"while collecting model input variables, the type could not be found for output variable '${inputVarName}'...")
							  null
						  }
						  
					  	  if (typedef != null) {
					  		  new ModelInputVariable(inputVarName, Array[(String,String)]((typeNameSpace,typeName)), hasModelPrefix)
					  	  } else {
					  		  null
					  	  }
					  } else {
						  null
					  } 
					  
					  if (inputVar != null) {
						  ctx.modelInputs(fldRef.field.toLowerCase()) = inputVar
					  } else {
						  logger.error(s"while collecting model input variables, the type could not be found for output variable '${fldRef.field}'...")
					  }
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


