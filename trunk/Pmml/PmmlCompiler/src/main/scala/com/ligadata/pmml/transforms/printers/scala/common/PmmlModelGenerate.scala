package com.ligadata.pmml.compiler

import org.apache.log4j.Logger

class PmmlModelGenerator(ctx : PmmlContext) extends LogTrait {

	/** Dump out the print elements for the syntax trees in the after transformation... before code generation */
	def generateDiagnostics() {
		val gBuffer : StringBuilder = new StringBuilder()
		
		val nodes : List[Option[PmmlExecNode]] = for (rootNodeName <- ctx.topLevelContainers) yield { ctx.pmmlExecNodeMap.apply(rootNodeName) }
		for (node <- nodes) {
			val fullclsName : String = node.getClass().getName()
			val clsparts : Array[String] = fullclsName.split('.')
			val clsName : String = clsparts.last
			clsName  match {
				  	case "xConstant" => {
				  			val constStr : String = node.asInstanceOf[xConstant].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(constStr)
				  			gBuffer.append("\n")
				  	}
					case "xHeader" => {
				  			val hdStr : String = node.asInstanceOf[xHeader].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(hdStr)
				  			gBuffer.append("\n")
				  	}
					case "xDataField" => {
				  			val dStr : String = node.asInstanceOf[xDataField].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(dStr)
				  			gBuffer.append("\n")
				  	}
					case "xDataDictionary" => {
				  			val dStr : String = node.asInstanceOf[xDataDictionary].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(dStr)
				  			gBuffer.append("\n")
				  	}
					case "xDerivedField" => {
				  			val dStr : String = node.asInstanceOf[xDerivedField].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(dStr)
				  			gBuffer.append("\n")
				  	}
					case "xTransformationDictionary" => {
				  			val xStr : String = node.asInstanceOf[xTransformationDictionary].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(xStr)
				  			gBuffer.append("\n")
				  	}
					case "xDefineFunction" => {
				  			val xStr : String = node.asInstanceOf[xDefineFunction].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(xStr)
				  			gBuffer.append("\n")
				  	}
					case "xParameterField" => {
				  			val xStr : String = node.asInstanceOf[xParameterField].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(xStr)
				  			gBuffer.append("\n")
				  	}
					case "xApply" => {
				  			val aStr : String = node.asInstanceOf[xApply].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(aStr)
				  			gBuffer.append("\n")
				  	}
					case "xFieldRef" => {
				  			val fStr : String = node.asInstanceOf[xFieldRef].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(fStr)
				  			gBuffer.append("\n")
				  	}
					case "xExtension" => {
				  			val eStr : String = node.asInstanceOf[xExtension].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(eStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesMap" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesMap].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesMapInline" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesMapInline].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesMapExternal" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesMapExternal].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesArray" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesArray].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesArrayExternal" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesArrayExternal].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xMapValuesArrayInline" => {
				  			val mStr : String = node.asInstanceOf[xMapValuesArrayInline].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xFieldColumnPair" => {
				  			val fStr : String = node.asInstanceOf[xFieldColumnPair].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(fStr)
				  			gBuffer.append("\n")
				  	}
					case "xMiningField" => {
				  			val mStr : String = node.asInstanceOf[xMiningField].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(mStr)
				  			gBuffer.append("\n")
				  	}
					case "xRuleSelectionMethod" => {
				  			val rStr : String = node.asInstanceOf[xRuleSelectionMethod].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(rStr)
				  			gBuffer.append("\n")
				  	}
					case "xRuleSetModel" => {
				  			val rStr : String = node.asInstanceOf[xRuleSetModel].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(rStr)
				  			gBuffer.append("\n")
				  	}
					case "xSimpleRule" => {
				  			val rStr : String = node.asInstanceOf[xSimpleRule].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(rStr)
				  			gBuffer.append("\n")
				  	}
					case "xScoreDistribution" => {
				  			val sStr : String = node.asInstanceOf[xScoreDistribution].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(sStr)
				  			gBuffer.append("\n")
				  	}
					case "xCompoundPredicate" => {
				  			val cStr : String = node.asInstanceOf[xCompoundPredicate].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(cStr)
				  			gBuffer.append("\n")
				  	}
					case "xSimplePredicate" => {
				  			val cStr : String = node.asInstanceOf[xSimplePredicate].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(cStr)
				  			gBuffer.append("\n")
				  	}
					case "xSimpleSetPredicate" => {
				  			val cStr : String = node.asInstanceOf[xSimplePredicate].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(cStr)
				  			gBuffer.append("\n")
				  	}
					case "xArray" => {
				  			val aStr : String = node.asInstanceOf[xArray].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(aStr)
				  			gBuffer.append("\n")
				  	}
					case "xValue" => {
				  			val vStr : String = node.asInstanceOf[xValue].asString(ctx)
				  			gBuffer.append("\n")
				  			gBuffer.append(vStr)
				  			gBuffer.append("\n")
				  	}
			  
			  
			}
		  
		  
		}
	  
	}



	def generateCode() {
	  
		val gBuffer : StringBuilder = new StringBuilder()
		
		val nodes : List[Option[PmmlExecNode]] = for (rootNodeName <- ctx.topLevelContainers) yield { ctx.pmmlExecNodeMap.apply(rootNodeName) }

		for (node <- nodes) {
			node match {
			  	case Some(node) => generateCode(Some(node), gBuffer)
				case _ => logger.error("One of the PMML root nodes is missing" )
			}			  
		}
	}
	
	
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
	
	/** 
	 *  
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
		
		/** Syntactic and Preliminary Semantic checks ... if this one fails, don't bother with the rest of it. */
		ctx.syntaxCheck
		if (ctx.ErrorCount > 0) {
			logger.error(s"\nThere are either syntax or simple semantic issues with this pmml... no point proceeding...\n")
			"No Source was generated"
		} else {	
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
	
			val classDeclsBuffer : StringBuilder = NodePrinterHelpers.classDecls(ctx, this)
			val modelClassBuffer : StringBuilder = NodePrinterHelpers.modelClass(ctx, this)
			
			codeBuffer.append(modelClassBuffer.toString)
			codeBuffer.append(classDeclsBuffer.toString)
			codeBuffer.toString
		}
}
	
	
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
	
    val  pmmlElementVistitorMap = Map[String, CodePrinter](
	      ("Constant" -> PmmlNode.mkPmmlConstant)
	    , ("Header" -> PmmlNode.mkPmmlHeader)
	    , ("Application" -> PmmlNode.mkPmmlApplication)
	    , ("DataDictionary" -> PmmlNode.mkPmmlDataDictionary)
	    , ("DataField" -> PmmlNode.mkPmmlDataField)
	    , ("Interval" -> PmmlNode.mkPmmlInterval)
	    , ("Value" -> PmmlNode.mkPmmlValue)
	    , ("TransformationDictionary" -> PmmlNode.mkPmmlTransformationDictionary)
	    , ("DerivedField" -> PmmlNode.mkPmmlDerivedField)
	    , ("Apply" -> PmmlNode.mkPmmlApply)
	    , ("FieldRef" -> PmmlNode.mkPmmlFieldRef)
	    , ("MapValues" -> PmmlNode.mkPmmlMapValues)
	    , ("FieldColumnPair" -> PmmlNode.mkPmmlFieldColumnPair)
	    , ("InlineTable" -> PmmlNode.mkPmmlInlineTable)
	    , ("row" -> PmmlNode.mkPmmlrow)
	    , ("RuleSetModel" -> PmmlNode.mkPmmlRuleSetModel)
	    , ("SimpleRule" -> PmmlNode.mkPmmlSimpleRule)
	    , ("ScoreDistribution" -> PmmlNode.mkPmmlScoreDistribution)
	    , ("CompoundPredicate" -> PmmlNode.mkPmmlCompoundPredicate)
	    , ("SimpleSetPredicate" -> PmmlNode.mkPmmlSimpleSetPredicate)
	    , ("SimplePredicate" -> PmmlNode.mkPmmlSimplePredicate)
	    , ("MiningSchema" -> PmmlNode.mkPmmlMiningSchema)
	    , ("MiningField" -> PmmlNode.mkPmmlMiningField)
	    , ("RuleSet" -> PmmlNode.mkPmmlRuleSet)
	    , ("RuleSelectionMethod" -> PmmlNode.mkPmmlRuleSelectionMethod)
	    , ("DefineFunction" -> PmmlNode.mkPmmlDefineFunction)
	    , ("ParameterField" -> PmmlNode.mkPmmlParameterField)
	    , ("Array" -> PmmlNode.mkPmmlArray))
	
}