package com.ligadata.pmml.xmlxform

import com.ligadata.pmml.syntaxtree.raw._
import com.ligadata.pmml.syntaxtree.cooked._
import com.ligadata.pmml.compiler._

class PmmlXform(val ctx : PmmlContext) extends LogTrait {

	/** iterate the pmmlNodeQueue, decorating each tree node as appropriate (see PmmlNode) to form
	 *  PmmlNode derivatives.
	 *  
	 *  Each node in the queue is navigated pre-order such that the basic PmmlNode derivative is 
	 *  built and pushed to the PmmlNodeStack.  The children then have the opportunity to either
	 *  directly decorate their parent state or build one of their kind and add it to the parent's
	 *  ArrayBuffer of children.  Some nodes do a little of each.
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
		  			dispatchPmmlToPmmlExecXform(this, node.qName, const)
		  	}
			case "PmmlHeader" => { 
		  			val hdNode : PmmlHeader = node.asInstanceOf[PmmlHeader]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, hdNode)
		  	}
			case "PmmlApplication" => { 
		  			val aNode : PmmlApplication = node.asInstanceOf[PmmlApplication]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlDataField" => { 
		  			val dNode : PmmlDataField = node.asInstanceOf[PmmlDataField]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlDataDictionary" => { 
		  			val dNode : PmmlDataDictionary = node.asInstanceOf[PmmlDataDictionary]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlDerivedField" => { 
		  			val dNode : PmmlDerivedField = node.asInstanceOf[PmmlDerivedField]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, dNode)
		  	}
			case "PmmlTransformationDictionary" => { 
		  			val xNode : PmmlTransformationDictionary = node.asInstanceOf[PmmlTransformationDictionary]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, xNode)
		  	}
			case "PmmlDefineFunction" => {
		  			val aNode : PmmlDefineFunction = node.asInstanceOf[PmmlDefineFunction]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlParameterField" => {
		  			val aNode : PmmlParameterField = node.asInstanceOf[PmmlParameterField]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlApply" => {
		  			val aNode : PmmlApply = node.asInstanceOf[PmmlApply]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlFieldRef" => { 
		  			val fNode : PmmlFieldRef = node.asInstanceOf[PmmlFieldRef]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, fNode)
		  	}
			case "PmmlExtension" => {
		  			val eNode : PmmlExtension = node.asInstanceOf[PmmlExtension]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, eNode)
		  	}
			case "PmmlMapValues" => {
		  			val mNode : PmmlMapValues = node.asInstanceOf[PmmlMapValues]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, mNode)
		  	}
			case "PmmlFieldColumnPair" => { 
		  			val fNode : PmmlFieldColumnPair = node.asInstanceOf[PmmlFieldColumnPair]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, fNode)
		  	}
			case "PmmlMiningField" => { 
		  			val mNode : PmmlMiningField = node.asInstanceOf[PmmlMiningField]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, mNode)
		  	}
			case "PmmlRuleSelectionMethod" => { 
		  			val rsNode : PmmlRuleSelectionMethod = node.asInstanceOf[PmmlRuleSelectionMethod]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, rsNode)
		  	}
			case "PmmlRuleSetModel" => {
		  			val rsmNode : PmmlRuleSetModel = node.asInstanceOf[PmmlRuleSetModel]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, rsmNode)
		  	}
			case "PmmlSimpleRule" => {
		  			val srNode : PmmlSimpleRule = node.asInstanceOf[PmmlSimpleRule]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, srNode)
		  	}
			case "PmmlScoreDistribution" => { 
		  			val sdNode : PmmlScoreDistribution = node.asInstanceOf[PmmlScoreDistribution]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, sdNode)
		  	}
			case "PmmlCompoundPredicate" => {
		  			val cpNode : PmmlCompoundPredicate = node.asInstanceOf[PmmlCompoundPredicate]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, cpNode)
		  	}
			case "PmmlSimplePredicate" => {
		  			val spNode : PmmlSimplePredicate = node.asInstanceOf[PmmlSimplePredicate]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, spNode)
		  	}
			case "PmmlSimpleSetPredicate" => {
		  			val sspNode : PmmlSimpleSetPredicate = node.asInstanceOf[PmmlSimpleSetPredicate]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, sspNode)
		  	}
			case "PmmlArray" => { 
		  			val aNode : PmmlArray = node.asInstanceOf[PmmlArray]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, aNode)
		  	}
			case "PmmlValue" => { 
		  			val vNode : PmmlValue = node.asInstanceOf[PmmlValue]
		  			dispatchPmmlToPmmlExecXform(this, node.qName, vNode)
		  	}
			case "PmmlMiningSchema" => {
					val msNode : PmmlMiningSchema = node.asInstanceOf[PmmlMiningSchema]
					dispatchPmmlToPmmlExecXform(this,node.qName, msNode)
			}
			case "PmmlRuleSet" => {
					val rsNode : PmmlRuleSet = node.asInstanceOf[PmmlRuleSet]
					dispatchPmmlToPmmlExecXform(this,node.qName, rsNode)
			}
			case _ => logger.warn(s"xform not handling nodes of type $clsName")
		}
	}

	/** Dispatcher for semantic analysis */
	def dispatchPmmlToPmmlExecXform(xformer : PmmlXform, qName : String, currentNode : PmmlNode) {

		var node : Option[PmmlExecNode] = qName match {
			case "Constant" => PmmlExecNode.mkPmmlExecConstant(ctx, currentNode.asInstanceOf[PmmlConstant])
			case "Header" => PmmlExecNode.mkPmmlExecHeader(ctx, currentNode.asInstanceOf[PmmlHeader])
			case "Application" => PmmlExecNode.mkPmmlExecApplication(ctx, currentNode.asInstanceOf[PmmlApplication])
			case "DataDictionary" => PmmlExecNode.mkPmmlExecDataDictionary(ctx, currentNode.asInstanceOf[PmmlDataDictionary])
			case "DataField" => PmmlExecNode.mkPmmlExecDataField(ctx, currentNode.asInstanceOf[PmmlDataField])
			case "Value" => PmmlExecNode.mkPmmlExecValue(ctx, currentNode.asInstanceOf[PmmlValue])
			case "Interval" => PmmlExecNode.mkPmmlExecInterval(ctx, currentNode.asInstanceOf[PmmlInterval])
			case "TransformationDictionary" => PmmlExecNode.mkPmmlExecTransformationDictionary(ctx, currentNode.asInstanceOf[PmmlTransformationDictionary])
			case "DerivedField" => PmmlExecNode.mkPmmlExecDerivedField(ctx, currentNode.asInstanceOf[PmmlDerivedField])
			case "DefineFunction" => PmmlExecNode.mkPmmlExecDefineFunction(ctx, currentNode.asInstanceOf[PmmlDefineFunction])
			case "ParameterField" => PmmlExecNode.mkPmmlExecParameterField(ctx, currentNode.asInstanceOf[PmmlParameterField])
			case "Apply" => PmmlExecNode.mkPmmlExecApply(ctx, currentNode.asInstanceOf[PmmlApply])
			case "FieldRef" => PmmlExecNode.mkPmmlExecFieldRef(ctx, currentNode.asInstanceOf[PmmlFieldRef])
			case "MapValues" => PmmlExecNode.mkPmmlExecMapValues(ctx, currentNode.asInstanceOf[PmmlMapValues])
			case "FieldColumnPair" => PmmlExecNode.mkPmmlExecFieldColumnPair(ctx, currentNode.asInstanceOf[PmmlFieldColumnPair])
			case "row" => PmmlExecNode.mkPmmlExecrow(ctx, currentNode.asInstanceOf[Pmmlrow])
			case "TableLocator" => PmmlExecNode.mkPmmlExecTableLocator(ctx, currentNode.asInstanceOf[PmmlTableLocator])
			case "InlineTable" => PmmlExecNode.mkPmmlExecInlineTable(ctx, currentNode.asInstanceOf[PmmlInlineTable])
			case "RuleSetModel" => PmmlExecNode.mkPmmlExecRuleSetModel(ctx, currentNode.asInstanceOf[PmmlRuleSetModel])
			case "MiningSchema" => PmmlExecNode.mkPmmlExecMiningSchema(ctx, currentNode.asInstanceOf[PmmlMiningSchema])
			case "MiningField" => PmmlExecNode.mkPmmlExecMiningField(ctx, currentNode.asInstanceOf[PmmlMiningField])
			case "SimpleRule" => PmmlExecNode.mkPmmlExecSimpleRule(ctx, currentNode.asInstanceOf[PmmlSimpleRule])
			case "ScoreDistribution" => PmmlExecNode.mkPmmlExecScoreDistribution(ctx, currentNode.asInstanceOf[PmmlScoreDistribution])
			case "RuleSet" => PmmlExecNode.mkPmmlExecRuleSet(ctx, currentNode.asInstanceOf[PmmlRuleSet])
			case "RuleSelectionMethod" => PmmlExecNode.mkPmmlExecRuleSelectionMethod(ctx, currentNode.asInstanceOf[PmmlRuleSelectionMethod])
			case "Array" => PmmlExecNode.mkPmmlExecArray(ctx, currentNode.asInstanceOf[PmmlArray])
			case "SimplePredicate" => PmmlExecNode.mkPmmlExecSimplePredicate(ctx, currentNode.asInstanceOf[PmmlSimplePredicate])
			case "CompoundPredicate" => PmmlExecNode.mkPmmlExecCompoundPredicate(ctx, currentNode.asInstanceOf[PmmlCompoundPredicate])
			case _ => None
		}

		node match {
			case Some(node) => {		  
				/** update the parent on the stack if appropriate */
				if (! ctx.pmmlExecNodeStack.isEmpty) {				  
					val top : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.top
					top match {
					  	case Some(top) => {
						  	var parent : PmmlExecNode = top.asInstanceOf[PmmlExecNode]
							parent.addChild(node)
					  	}
					  	case _ => logger.error("Fantastic... there are None elements on the stack!")
					}
				}
				ctx.pmmlExecNodeStack.push(Some(node))
				val aNewNode : PmmlExecNode = node
				currentNode.Children.foreach((child) => {
					xformer.transform1(child.asInstanceOf[PmmlNode])
				})
				val completedXformNode : Option[PmmlExecNode] = ctx.pmmlExecNodeStack.pop().asInstanceOf[Option[PmmlExecNode]]
				completedXformNode match {
					case Some(completedXformNode) => {
						if (ctx.topLevelContainers.contains(completedXformNode.qName)) {
							ctx.pmmlExecNodeMap(completedXformNode.qName) = Some(completedXformNode)
						}
					}
					case _ => { /** comment out this case once tested */
						 logger.trace(s"node $currentNode.qName does not have children or had children subsumed by the parent")
					}
				}
			}
			case _ => {
				logger.debug(s"node $currentNode make did not produce a PmmlExecNode derivative")
			}
		}
	}
	
}