package com.ligadata.pmml.node.print

import org.apache.log4j.Logger
import com.ligadata.pmml.syntaxtree.cooked._
import com.ligadata.pmml.node.print._
import com.ligadata.pmml.compiler._
import com.ligadata.pmml.support._

class PmmlModelGenerator(ctx : PmmlContext) extends LogTrait {

	/** 
	 *  Create the Scala source file from the parsed and transformed pmml nodes.  Currently only RuleSetModel is supported.
	 *  @return scala source string
	 */
	def ComposePmmlModelSource : String = {
		/** fix upd the ctx before proceeding */
		val rsm : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("RuleSetModel") 
		rsm match {
		  case Some(rsm) => {
			  val rsmInst : xRuleSetModel = rsm.asInstanceOf[xRuleSetModel]
			  ctx.MiningSchemaMap(rsmInst.MiningSchemaMap)
		  }
		  case _ => logger.error(s"there was no RuleSetModel avaialble... whoops!\n")
		}
		
		/** Register the parameters */
		ctx.RegisterMessages
		/** 
		 *  Pass through the TransactionDictionary xDerivedFields seeking any message or container definitions. 
		 *  These are added to the ctx.containersInScope just like the parameters field values in the datadictionary
		 */
		ctx.collectContainers
		
		/** collect the udf functions from the TransactionDictinary, etc. */
		ctx.collectUdfs()
		/** transform the 'if' apply elements rewriting the child list by removing the true and false elements at the list end.  */
		ctx.transformTopLevelApplyNodes()
		
		/** collect additional information needed to decorate the runtime RuleSetModel class */
		ctx.ruleSetModelInfoCollector()
		/** ditto for SimpleRule class */
		ctx.simpleRuleInfoCollector() 
		
		/** collect the input field and output fields for model definition generation */
		ctx.collectModelInputVars
		ctx.collectModelOutputVars
	
		/** 
		 *  Build the source code returning StringBuilders for the pieces.  The
		 *  classDecls for the RuleSetModel, Rule and Derived fields are processed
		 *  first so that the necessary strings for instantiation of these classes
		 *  can be collected in the classInstanceMakerMap in the ctx.
		 *  
		 *  They are printed in the other order however with the model class first.
		 */
		val codeBuffer : StringBuilder = new StringBuilder()

		val classDeclsBuffer : StringBuilder = classDecls
		val modelClassBuffer : StringBuilder = modelClass
		
		codeBuffer.append(modelClassBuffer.toString)
		codeBuffer.append(classDeclsBuffer.toString)
		codeBuffer.toString
	}
	
	/**
	 * 	Answer a StringBuilder filled with the PmmlModel's Scala class representation.
	 *  @return a StringBuilder
	 */
	def modelClass : StringBuilder = {
		val clsBuffer : StringBuilder = new StringBuilder()
			
		clsBuffer.append(NodePrinterHelpers.modelClassComment(ctx, this))
		clsBuffer.append(NodePrinterHelpers.objBody(ctx, this))
		clsBuffer.append(NodePrinterHelpers.modelClassBody(ctx, this))
		/** 
		 *  End of Application Class generation
		 */
		clsBuffer.append(s"}\n")
 		
		clsBuffer
 	}
	
	
	/**
	 *      Answer a StringBuilder containing Scala representations for the derived fields
	 *      in the TransformationDictionary, the SimpleRule classes, and the RuleSetModel 
	 *      class.
	 *      
	 *      @return a StringBuilder
	 */

	def classDecls : StringBuilder =  {
		val classBuffer: StringBuilder = new StringBuilder
		classBuffer.append(s"\n")
		classBuffer.append(s"/*************** Derived Field Class Definitions ***************/\n\n")
		val xDictNode : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("TransformationDictionary")
		generateCode1(xDictNode, classBuffer, this, CodeFragment.DERIVEDCLASS)
		classBuffer.append(s"\n")
		classBuffer.append(s"/*************** SimpleRule Class Definitions ***************/\n\n")
		val ruleSetModelNode : Option[PmmlExecNode] = ctx.pmmlExecNodeMap.apply("RuleSetModel")
		generateCode1(ruleSetModelNode, classBuffer, this, CodeFragment.RULECLASS)
		classBuffer.append(s"\n")
		classBuffer.append(s"/*************** RuleSetModel Class Definition ***************/\n\n")
		generateCode1(ruleSetModelNode, classBuffer, this, CodeFragment.RULESETCLASS)
		
		classBuffer
	}
	

	
	/**
	 *  Generate Scala code from the supplied node, appending the result to the supplied StringBuilder instance.
	 */
	def generateCode1(node : Option[PmmlExecNode], gBuffer: StringBuilder, generator : PmmlModelGenerator, kind : CodeFragment.Kind) {
		node match {
			case Some(node) => {
				node.qName  match {
				  	case "Constant" => { 
				  			val aConst : xConstant = node.asInstanceOf[xConstant]	
				  			val rep : String = aConst.asString(ctx)
				  			gBuffer.append(aConst.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "Header" => { 
				  			val hdNode : xHeader = node.asInstanceOf[xHeader]
				  			gBuffer.append(hdNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "DataField" => { 
				  			val dNode : xDataField = node.asInstanceOf[xDataField]
				  			gBuffer.append(dNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "DataDictionary" => { 
				  			val dNode : xDataDictionary = node.asInstanceOf[xDataDictionary]
				  			gBuffer.append(dNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "DerivedField" => { 
				  			val dNode : xDerivedField = node.asInstanceOf[xDerivedField]
				  			gBuffer.append(dNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "TransformationDictionary" => { 
				  			val xNode : xTransformationDictionary = node.asInstanceOf[xTransformationDictionary]
				  			gBuffer.append(xNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "DefineFunction" => {
				  			val aNode : xDefineFunction = node.asInstanceOf[xDefineFunction]
				  			gBuffer.append(aNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "ParameterField" => {
				  			val aNode : xParameterField = node.asInstanceOf[xParameterField]
				  			gBuffer.append(aNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "Apply" => {
				  			val aNode : xApply = node.asInstanceOf[xApply]
				  			gBuffer.append(aNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "FieldRef" => { 
				  			val fNode : xFieldRef = node.asInstanceOf[xFieldRef]
				  			gBuffer.append(fNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "Extension" => {
				  			val eNode : xExtension = node.asInstanceOf[xExtension]
				  			gBuffer.append(eNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesMap" => {
				  			val mNode : xMapValuesMap = node.asInstanceOf[xMapValuesMap]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesMapInline" => {
				  			val mNode : xMapValuesMapInline = node.asInstanceOf[xMapValuesMapInline]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesMapExternal" => {
				  			val mNode : xMapValuesMapExternal = node.asInstanceOf[xMapValuesMapExternal]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesArray" => {
				  			val mNode : xMapValuesArray = node.asInstanceOf[xMapValuesArray]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesArrayExternal" => {
				  			val mNode : xMapValuesArrayExternal = node.asInstanceOf[xMapValuesArrayExternal]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MapValuesArrayInline" => {
				  			val mNode : xMapValuesArrayInline = node.asInstanceOf[xMapValuesArrayInline]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "FieldColumnPair" => { 
				  			val fNode : xFieldColumnPair = node.asInstanceOf[xFieldColumnPair]
				  			gBuffer.append(fNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "MiningField" => { 
				  			val mNode : xMiningField = node.asInstanceOf[xMiningField]
				  			gBuffer.append(mNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "RuleSelectionMethod" => { 
				  			val rsNode : xRuleSelectionMethod = node.asInstanceOf[xRuleSelectionMethod]
				  			gBuffer.append(rsNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "RuleSetModel" => {
				  			val rsmNode : xRuleSetModel = node.asInstanceOf[xRuleSetModel]
				  			gBuffer.append(rsmNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  			ctx.MiningSchemaMap(rsmNode.miningSchemaMap)
				  	}
					case "RuleSet" => {
				  			val rsNode : xRuleSet = node.asInstanceOf[xRuleSet]
				  			gBuffer.append(rsNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "SimpleRule" => {
				  			val srNode : xSimpleRule = node.asInstanceOf[xSimpleRule]
				  			gBuffer.append(srNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "ScoreDistribution" => { 
				  			val sdNode : xScoreDistribution = node.asInstanceOf[xScoreDistribution]
				  			gBuffer.append(sdNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "CompoundPredicate" => {
				  			val cpNode : xCompoundPredicate = node.asInstanceOf[xCompoundPredicate]
				  			gBuffer.append(cpNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "SimplePredicate" => {
				  			val spNode : xSimplePredicate = node.asInstanceOf[xSimplePredicate]
				  			gBuffer.append(spNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "SimpleSetPredicate" => {
				  			val sspNode : xSimpleSetPredicate = node.asInstanceOf[xSimpleSetPredicate]
				  			gBuffer.append(sspNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "Array" => { 
				  			val aNode : xArray = node.asInstanceOf[xArray]
				  			gBuffer.append(aNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case "Value" => { 
				  			val vNode : xValue = node.asInstanceOf[xValue]
				  			gBuffer.append(vNode.codeGenerator(ctx, this, kind, Traversal.PREORDER))
				  	}
					case _ => {
						val nodeName : String = node.qName
						PmmlError.logError(ctx, s"pmml model generate ... un handled node ... name = $nodeName")
					}
				}
			}
			case _ => PmmlError.logError(ctx, s"pmml model generate ... UnknownNode ... name = $node.qName")
		}
	}
	
	
	/** 
	 *  For diagnostic purposes... generate aspects of the pmml transformation including
	 *  <ul>
	 *  <li>Header</li>
	 *  <li>DataDictionary</li>
	 *  <li>TransformationDictionary</li>
	 *  <li>RuleSetModel</li>
	 *  </ul>
	 *  
	 */
	def generateCode (node : Option[PmmlExecNode], gBuffer : StringBuilder) {
	  
		val nodeName : String = node match {
			case Some(node) => node.qName
			case _ => "UnknownNode"
		}
		nodeName   match {
			case "Header" => {
				gBuffer.append("Header code:\n")
				generateCode1(node, gBuffer, this, CodeFragment.VARDECL)
			} 
			case "DataDictionary" => {
				gBuffer.append("DataDictionary code:\n")
				generateCode1(node, gBuffer, this, CodeFragment.VARDECL)
			} 
			case "TransformationDictionary" => {
				gBuffer.append("TransformationDictionary code:\n")
				generateCode1(node, gBuffer, this, CodeFragment.VARDECL)
				generateCode1(node, gBuffer, this, CodeFragment.DERIVEDCLASS)
			} 
			case "RuleSetModel" => {
				gBuffer.append("\nRule code:\n\n")
				generateCode1(node, gBuffer, this, CodeFragment.RULECLASS)
				gBuffer.append("\nRuleSetModel code:\n\n")
				generateCode1(node, gBuffer, this, CodeFragment.RULESETCLASS)
			} 
		  
		}
	}
	

}