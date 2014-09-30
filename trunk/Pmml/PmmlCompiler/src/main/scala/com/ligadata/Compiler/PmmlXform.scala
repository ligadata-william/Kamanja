package com.ligadata.Compiler

import org.apache.log4j.Logger

class PmmlXform(val ctx : PmmlContext) extends LogTrait {

	/** iterate the pmmlNodeQueue, decorating each tree node as appropriate (see PmmlNode) to form
	 *  PmmlNode derivatives.
	 *  
	 *  Each node in the queue is navigated pre-order such that the basic PmmlNode derivative is 
	 *  built and pushed to the PmmlNodeStack.  The children then have the opportunity to either
	 *  directly decorate their parent state or build one of their kind and add it to the parent's
	 *  ArrayBuffer of children.  Some nodes do a little of each.
	 *  
	 */
	def transform() {  
		val gBuffer : StringBuilder = new StringBuilder()
		for (node <- ctx.pmmlNodeQueue) {
			transform(node, gBuffer)		
		}
	}
	
	
	def transform (node : PmmlNode, gBuffer : StringBuilder) {	
		val fullclsName : String = node.getClass().getName()
		val clsparts : Array[String] = fullclsName.split('.')
		val clsName : String = clsparts.last
		clsName match {
			case "PmmlHeader" => {
				gBuffer.append("Header code:\n")
				transform1(node)
			} 
			case "PmmlDataDictionary" => {
				gBuffer.append("DataDictionary code:\n")
				transform1(node)
			} 
			case "PmmlTransformationDictionary" => {
				gBuffer.append("TransformationDictionary code:\n")
				transform1(node)
			} 
			case "PmmlRuleSetModel" => {
				val hdNode : PmmlRuleSetModel = node.asInstanceOf[PmmlRuleSetModel]
				gBuffer.append("RuleSetModel code:\n")
				transform1(hdNode)
			} 
		  
		}
	}
	
	def transform1(node : PmmlNode) {
		val fullclsName : String = node.getClass().getName()
		val clsparts : Array[String] = fullclsName.split('.')
		val clsName : String = clsparts.last
		clsName match {
		  	case "PmmlConstant" => { 
		  			val const : PmmlConstant = node.asInstanceOf[PmmlConstant]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, const)
		  	}
			case "PmmlHeader" => { 
		  			val hdNode : PmmlHeader = node.asInstanceOf[PmmlHeader]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, hdNode)
		  	}
			case "PmmlApplication" => { 
		  			val aNode : PmmlApplication = node.asInstanceOf[PmmlApplication]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlDataField" => { 
		  			val dNode : PmmlDataField = node.asInstanceOf[PmmlDataField]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlDataDictionary" => { 
		  			val dNode : PmmlDataDictionary = node.asInstanceOf[PmmlDataDictionary]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlDerivedField" => { 
		  			val dNode : PmmlDerivedField = node.asInstanceOf[PmmlDerivedField]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlTransformationDictionary" => { 
		  			val xNode : PmmlTransformationDictionary = node.asInstanceOf[PmmlTransformationDictionary]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, xNode)
		  	}
			case "PmmlDefineFunction" => {
		  			val aNode : PmmlDefineFunction = node.asInstanceOf[PmmlDefineFunction]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlParameterField" => {
		  			val aNode : PmmlParameterField = node.asInstanceOf[PmmlParameterField]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlApply" => {
		  			val aNode : PmmlApply = node.asInstanceOf[PmmlApply]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlFieldRef" => { 
		  			val fNode : PmmlFieldRef = node.asInstanceOf[PmmlFieldRef]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, fNode)
		  	}
			case "PmmlExtension" => {
		  			val eNode : PmmlExtension = node.asInstanceOf[PmmlExtension]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, eNode)
		  	}
			case "PmmlMapValues" => {
		  			val mNode : PmmlMapValues = node.asInstanceOf[PmmlMapValues]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, mNode)
		  	}
			case "PmmlFieldColumnPair" => { 
		  			val fNode : PmmlFieldColumnPair = node.asInstanceOf[PmmlFieldColumnPair]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, fNode)
		  	}
			case "PmmlMiningField" => { 
		  			val mNode : PmmlMiningField = node.asInstanceOf[PmmlMiningField]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, mNode)
		  	}
			case "PmmlRuleSelectionMethod" => { 
		  			val rsNode : PmmlRuleSelectionMethod = node.asInstanceOf[PmmlRuleSelectionMethod]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, rsNode)
		  	}
			case "PmmlRuleSetModel" => {
		  			val rsmNode : PmmlRuleSetModel = node.asInstanceOf[PmmlRuleSetModel]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, rsmNode)
		  	}
			case "PmmlSimpleRule" => {
		  			val srNode : PmmlSimpleRule = node.asInstanceOf[PmmlSimpleRule]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, srNode)
		  	}
			case "PmmlScoreDistribution" => { 
		  			val sdNode : PmmlScoreDistribution = node.asInstanceOf[PmmlScoreDistribution]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, sdNode)
		  	}
			case "PmmlCompoundPredicate" => {
		  			val cpNode : PmmlCompoundPredicate = node.asInstanceOf[PmmlCompoundPredicate]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, cpNode)
		  	}
			case "PmmlSimplePredicate" => {
		  			val spNode : PmmlSimplePredicate = node.asInstanceOf[PmmlSimplePredicate]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, spNode)
		  	}
			case "PmmlSimpleSetPredicate" => {
		  			val sspNode : PmmlSimpleSetPredicate = node.asInstanceOf[PmmlSimpleSetPredicate]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, sspNode)
		  	}
			case "PmmlArray" => { 
		  			val aNode : PmmlArray = node.asInstanceOf[PmmlArray]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlValue" => { 
		  			val vNode : PmmlValue = node.asInstanceOf[PmmlValue]
		  			ctx.dispatchPmmlToPmmlExecXform(this, node.qName, vNode)
		  	}
			case "PmmlMiningSchema" => {
					val msNode : PmmlMiningSchema = node.asInstanceOf[PmmlMiningSchema]
					ctx.dispatchPmmlToPmmlExecXform(this,node.qName, msNode)
			}
			case "PmmlRuleSet" => {
					val rsNode : PmmlRuleSet = node.asInstanceOf[PmmlRuleSet]
					ctx.dispatchPmmlToPmmlExecXform(this,node.qName, rsNode)
			}
			case _ => logger.warn(s"xform not handling nodes of type $clsName")
		}
	}

}