package com.ligadata.Compiler

import scala.collection.mutable._
import scala.collection.immutable.{ Set }
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import scala.reflect.runtime.universe._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._
//import com.ligadata.OnLEPBankPoc._

/** 
 *  1) Build a function typestring from the apply node and its children (function arguments) to locate the appropriate
 *  function in the metadata.
 *  2) Determine the return type for this function.  Use it to update the parent derived field (if needed).  
 *  	a) If this function is Iterable the first argument will be considered the receiver, the second a function
 *   		that operates on the Iterables elements and the remaining arguments used to select the function
 *   		filter, map, retain ... etc function.
 *      b) For map functions where the return type of the Iterable returned is different than the receiver, the
 *      	type will be inferred from the transformation function used on each element.  
 *       
 *       	For example, consider this map function that converts an array inpatient claims (a struct) to an array of doubles
 *        
 *        		<DerivedField name="OutClaimIcd9DgnsFilter" dataType="Array" optype="categorical">
 *          		<Apply function="ContainerMap">
 *            			<FieldRef field="inPatientClaims"/>
 *               		<Constant dataType="fIdent">Sum</Constant>
 *                 		<Constant dataType="ident">Clm_Pmt_Amt</Constant>
 *                   	<Constant dataType="ident">Nch_Prmry_Pyr_Clm_Pd_Amt</Constant>
 *                    	<Constant dataType="ident">Nch_Bene_Blood_Ddctbl_Lblty_Am</Constant>
 *                     	<Constant dataType="ident">Nch_Bene_Ptb_Ddctbl_Amt</Constant>
 *                      <Constant dataType="ident">Nch_Bene_Ptb_Coinsrnc_Amt</Constant>
 *                      <Constant dataType="integer">1</Constant> 
 *                  </Apply>
 *              </DerivedField>
 *              
 *           A function key for the "fIdent", Sum, would be prepared consisting of "Sum(double,double,double,double,double,integer)"
 *           If there is not an exact match, a new key will be prepared that relaxes the argument types as necesary.  In this case
 *           the new key would be "Sum(double,double,double,double,double,double)".  If that fails, we are done, complaining about
 *           the lack of a suitable function for this usage.
 *           
 *           Obviously the degree of sophistication employed for determining a matching type can be complex.  The "implicit"-ness
 *           used to infer the proper function and its types will be a continuing improvement project.
 *		c) When the function is not iterable all arguments are treated as arguments to the named apply function 
 */

class FunctionSelect(val ctx : PmmlContext, val mgr : MdMgr, val node : xApply) {
  
	def selectFunction : (FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = {
	  	var selectedFcn : FunctionDef = null
	  	var argTypes : Array[(String,Boolean,BaseTypeDef)] = null
	  	val (firstFcn,onlyOne) : (FunctionDef,Boolean) = representativeFcn(node.function)
	  	//if (onlyOne) { /** if only one function, its arguments either match or they don't */
	  	//	selectedFcn = firstFcn
	  	//} else {
		  	val (fcndef, argInfo) : (FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = if (firstFcn != null) {
		  		val isIterable = firstFcn.features.contains(FcnMacroAttr.ITERABLE)
		  		if (isIterable) selectIterableFcn else selectSimpleFcn
		  	} else {
		  		val nm : String = node.function
		  		ctx.logger.error(s"Unable to find function by the name '$nm' in the metadata.")
		  		(null,null)
		  	}
		  	selectedFcn = fcndef
		  	argTypes = argInfo
	  	//}
	  	(selectedFcn, argTypes)
	}
	
	def selectSimpleFcn : (FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = {
	  	/** create a search key from the function name and its children (the arguments). */
	  	ctx.logger.trace(s"selectSimpleFcn ... search mdmgr for ${node.function}...")
	  	var argTypes : Array[(String,Boolean,BaseTypeDef)] = collectArgKeys()
	  	var argTypesExpanded : Array[(String,Boolean,BaseTypeDef)] = collectArgKeys(true)
	  	var simpleKey : String = buildSimpleKey(argTypes.map( argPair => argPair._1))
	  	val nmspcsSearched : String = ctx.NameSpaceSearchPath
	  	ctx.logger.trace(s"selectSimpleFcn ... key used for mdmgr search = '$nmspcsSearched.$simpleKey'...")
	  	//simpleKey = "Get(EnvContext,String,Long)"
	  	var funcDef : FunctionDef = ctx.MetadataHelper.FunctionByTypeSig(simpleKey)
	  	if (funcDef == null) {
	  		val simpleKeysToTry : Array[String] = relaxSimpleKey(argTypes)
	  		breakable {
	  		  	simpleKeysToTry.foreach( key => {
	  		  		ctx.logger.trace(s"selectSimpleFcn ...searching mdmgr with a relaxed key ... $key")
	  		  		funcDef = ctx.MetadataHelper.FunctionByTypeSig(key)
	  		  		if (funcDef != null) {
	  		  			ctx.logger.trace(s"selectSimpleFcn ...found funcDef with $key")
	  		  			break
	  		  		}
	  		  	})	  		  
	  		}
	  	}
	  	val foundDef : String = if (funcDef != null)  "YES" else "NO!!"
	  	ctx.logger.trace(s"selectSimpleFcn ...funcDef produced? $foundDef ")
	  	(funcDef,argTypes)
	}
	
	/** 
	 *  Note: When the functions with nmspc.name hae the ITERABLE feature, there are two searches
	 *  performed:
	 *  	1) for the container function 
	 *   	2) for the element function that operates on the container elements
	 *    
	 *  FIXME: This code builds below but is NOT correct.  Fix it. 
	 *  
	 */
	def selectIterableFcn : (FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = {
	  	var iterableFcn : FunctionDef = null
	  	var argTypes : Array[(String,Boolean,BaseTypeDef)] =  collectArgKeys()
	  	var elemFKey : String  = buildIterableKey(true)
	  	var fKey : String  = buildIterableKey(false)
	  	var elementFcn : FunctionDef = selectElementFunction(elemFKey)
	  	if (elementFcn == null) {
		  	elemFKey = relaxIterableKey(true) /** redo the elementFcnKey when true */
	  		elementFcn = ctx.MetadataHelper.FunctionByTypeSig(elemFKey)
	  	}
	  	if (elementFcn != null) {
	  		iterableFcn = selectIterableFcn1(fKey, elementFcn)
	  		if (iterableFcn == null) {
	  			fKey = relaxIterableKey(true) /** redo the Iterable fcn key too (chg container type to its parent or Any) */
	  			iterableFcn = selectIterableFcn1(fKey, elementFcn)
	  		}
	  	}
	  	(iterableFcn, argTypes)
	} 
	
	def selectIterableFcn1(fKey : String, elementFcn : FunctionDef) : FunctionDef = {
	  	var iterableFcn : FunctionDef = null
	  	iterableFcn
	} 
	
	
	def selectElementFunction(elemFKey : String) : FunctionDef = {
	  	/** create a search key from the function name and its children (the arguments). */
	  	var funcDef : FunctionDef = ctx.MetadataHelper.FunctionByTypeSig(elemFKey)
	  	funcDef
	}
	
	/** 
	 *  Determine if there is at least one function by this name in the metadata and return one of them 
	 *  This gives the caller an idea if this is a standard "simple" function or what is known as an
	 *  "iterable" function.  Iterable functions have as the name suggests the Iterable trait.  Actually
	 *  the first argument to the function is known as the "receiver" actually has this Iterable trait.
	 *  
	 *  When one of these is found, the formatting of the function print take forms like these:
	 *  	iterableObj.map(itm => sum(itm.a, itm.b, itm.c))
	 *   	iterableObj.filter(itm => between(itm.a,lowbound,hibound,inclusive))
	 *    
	 *  Were the iterableObject's items "map-based" (as opposed to "fixed"), something like these
	 *  would be generated:
	 *  	iterableObj.map(itm => sum(itm(a), itm(b), itm(c)))
	 *   	iterableObj.filter(itm => between(itm(a),lowbound,hibound,inclusive))
	 *    
	 *  @param name of the function(s) sought, possibly namespace qualified)
	 *  @return if found, the FunctionDef of one (possibly the only) functions with this name and 
	 *  	a Boolean that if true says that there is but ONE function by this name (i.e., we're done)
	 *  
	 */
	def representativeFcn(name : String) : (FunctionDef,Boolean) = {
	  	val availableFcns : Set[FunctionDef] = ctx.MetadataHelper.getFunctions(name)
	  	val firstFcnOpt : Option[FunctionDef] = availableFcns.headOption
	  	val (firstFcn, onlyOne) : (FunctionDef, Boolean) = firstFcnOpt match {
	  	  case Some(firstFcnOpt) => {
	  		  if (availableFcns.size ==1) (firstFcnOpt,true) else (firstFcnOpt,false)
	  	  }
	  	  case _ => (null,false)
	  	}
	  	(firstFcn,onlyOne)
	}
	
	/** 
	 *  Answer an array of function keys that could match a specific version
	 *  of a function that has this simple function name (or namespace qualified name).
	 *  
	 *  @param name : the function name search key to locate the functions that 
	 *  	available with this name.
	 *  @return the search keys that could be used to locate an
	 */
	def FunctionKeysThatWouldMatch(name : String) : Array[String] = {
	  	val availableFcns : Set[FunctionDef] = ctx.MetadataHelper.getFunctions(name)	  	
	  	val fcnKeys : Array[String] = if (availableFcns != null && availableFcns.size > 0) {
	  	  availableFcns.map( fcndef => fcndef.typeString).toArray
	  	} else {
	  		Array[String]()
	  	}
	  	fcnKeys
	}
	
	/** Create a metadata search key from the apply's function name and ALL the child arguments. */
	def collectArgKeys(expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {

	  	val noChildren : Int = node.Children.size
	  	var argTypes : Array[(String,Boolean,BaseTypeDef)] = Array[(String,Boolean,BaseTypeDef)]()
	  	node.Children.foreach ((child) => {
	  		argTypes ++= typeStringForSimple(child, expandCompoundFieldTypes)		
  		})
	  	argTypes
	}
	
	/** Create a type string from the key, sans namespace */
	def buildSimpleKey(argtypes : Array[String]) : String = {
		val keyBuff : StringBuilder = new StringBuilder()
	  	keyBuff.append(node.function)
	  	keyBuff.append('(')
	  	var cnt : Int = 0
	  	argtypes.foreach (arg => {
	  		keyBuff.append(arg)
	  		cnt += 1
	  		if (cnt < argtypes.size) keyBuff.append(',')
	  	})
	  	keyBuff.append(')')
	  	val simpleKey : String = keyBuff.toString
	  	simpleKey
	}
	
	/** One or more functions with node.function as name exists and has been
	 *  determined to be an "Iterable" function.  This means that any of the 
	 *  functions with this name must be "Iterable" and as such we build two 
	 *  keys for both the element function and the Iterable function that uses
	 *  it.  Supply the correct boolean value to choose which should be built. 
	 */
	def buildIterableKey(buildElementFcnKey : Boolean) : String = {
	  	/** FIXME: provide an implementation for this */
	  	if (buildElementFcnKey) {
	  	  
	  	} else {
	  	  
	  	}
	  
	  	""
	}
	
	/** 
	 *  If the straight forward search key produced does not produce a FunctionDef, this more
	 *  elaborate mechanism is used to try to relax the argument types. Answer an array of 
	 *  additional keys to utilize for function type search.
	 *  
	 *  NOTE: This is a work in progress. First implementation will look for scalar arguments
	 *  of different widths and relax the width iff there are functions with a wider width
	 *  than was proposed in the first key produced.
	 *  
	 *  Current method:
	 *  	relaxation 1) If there are containers in the argument list, collect the superclasses
	 *   			for each of them, find the first abstract or trait and use it
	 *      relaxation 2) Change containers to Any
	 *      relaxation 3) If there are scalars, broaden their width
	 *      relaxation 4) Make all arguments "Any"
	 *      
	 *  Improvement Idea:
	 *  
	 *  Determine the class inheritance hierarchy for any Container.  For example, consider this function:
	 *   
	 *     	def Put(gCtx : EnvContext, containerId : String, key : Float, value : BaseContainer) : Boolean = {
	 * 			gCtx.setObject(containerId, key.toString, value) 
	 * 			true
  	 *		}
  	 *
	 *  With the relaxation rules currently in use, this function will not be found.  The BaseContainer subclasses
	 *  in use in the models cannot be easily derive 
	 */

	def relaxSimpleKey(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Array[String] = {
	  	/** 
	  	 *  FIXME: provide an implementation for this 
	  	 *  buildSimpleKey(argtypes : Array[String]) : String
	  	 */

	  	var relaxedKeys : ArrayBuffer[String] = ArrayBuffer[String]()
	  	/** 1. */
	  	/** 
	  	 *  Change the containers in the argTypes to use the first base class they have that is either an abstract
	  	 *  class or trait 
	  	 */
	  	
	  	val argsWithPromotedContainerClasses : Array[(String,Boolean,BaseTypeDef)] = relaxToFirstTraitOrAbstractClass(argTypes)
	  	val relaxedTypes1 : Array[String] = argsWithPromotedContainerClasses.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		arg
	  	})
	  	relaxedKeys += buildSimpleKey(relaxedTypes1)
	  		  	
	  	/** 2. */
	  	val relaxedTypes2 : Array[String] = argTypes.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		if (isContainer) {
	  			"Any"
	  		} else {
	  			arg
	  		}
	  	})
	  	relaxedKeys += buildSimpleKey(relaxedTypes2)
	  		  	
	  	/** 3. */
	  	val relaxedTypes3 : Array[String] = argTypes.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		arg match {
	  		  case "Int" => "Long"
	  		  case "Float" => "Double"
	  		  case _ => arg
	  		}
	  	})
	  	relaxedKeys += buildSimpleKey(relaxedTypes3)
	  	
	  	/** 4. */
	  	val relaxedTypes4 : Array[String] = argTypes.map( argInfo => {
	  		"Any"
	  	})
	  	relaxedKeys += buildSimpleKey(relaxedTypes4)
	  	
	  	ctx.logger.trace("...relaxed keys:")
	  	relaxedKeys.foreach( key => ctx.logger.trace(s"$key"))
	  
	  	relaxedKeys.toArray
	}
	
	/** 
	 *  Replicate a the supplied triple cnt times answering an array.
	 */
  	def replicate(item : (String,Boolean,BaseTypeDef), cnt : Int) : Array[(String,Boolean,BaseTypeDef)] = {
  		var array : ArrayBuffer[(String,Boolean,BaseTypeDef)] = ArrayBuffer[(String,Boolean,BaseTypeDef)]()
  		
  		for (i <- 1 to cnt) { array += item}
  		array.toArray
  	}
  	
	/** Prepare a new set of argTypes that promote the container typedef to their respective abstract class
	 *  superclass or trait.
	 */
	def relaxToFirstTraitOrAbstractClass(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Array[(String,Boolean,BaseTypeDef)] = {  
	  	val baseArgTypes : Map[String, Array[(String, ClassSymbol, Type)]] = collectContainerSuperClasses(argTypes)  	
	  	val newArgTypes : Array[(String,Boolean,BaseTypeDef)] = argTypes.map( argType => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argType
	  		val useThisTuple : (String,Boolean,BaseTypeDef) = if (isContainer) {
	  			val argTypeInfo = baseArgTypes(arg)
				var newType : String = null
		  		breakable {
		  			argTypeInfo.foreach( triple => {
		  				val (clssym, symbol, typ) : (String, ClassSymbol, Type) = triple
		  				val typName = 
		  				if (symbol.isAbstractClass || symbol.isTrait) {
		  					newType = symbol.fullName
		  					break
		  				}	  				  
		  			})
		  		}
	  			(newType, isContainer, elem)
	  		} else {
	  			(arg, isContainer, elem)
	  		}
	  		useThisTuple
	  	})
	  	newArgTypes
	}
	
	/** 
	 *  For any ContainerTypesDefs, collect the super classes for each one in the argTypes.  Answer an array
	 *  of (type name, class symbol, type symbol) triples for each superclass) for each arg type.
	 *  @param argTypes (the type (class) name for the container, if it is a container, the metadata for this type)
	 *  @return for each argType an array that consists of (type, class symbol, type symbol)  
	 */
	def collectContainerSuperClasses(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Map[String, Array[(String, ClassSymbol, Type)]] = {

		val mirror = runtimeMirror(this.getClass.getClassLoader)
	  	val containerNamefullPkgNames : Array[(String,String)] = argTypes.filter(arg => arg._3.isInstanceOf[ContainerTypeDef]).map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		(arg, elem.typeString)
	  	})

	  	val containersWithSuperClasses : Array[(String,Array[(String, ClassSymbol, Type)])] = containerNamefullPkgNames.map( pair => {
	  		val (nm, fqClassname) : (String, String) = pair
	  		// Convert class name into a class
	  		val clz = Class.forName(fqClassname)
			// Convert class into class symbol
			val clsSymbol = mirror.classSymbol(clz)
			// Info about the class
			val isTrait = clsSymbol.isTrait			 
			val isAbstractClass = clsSymbol.isAbstractClass
			val isModule = clsSymbol.isModule
			// Convert the class symbol into a Type
			val clsType = clsSymbol.toType
	  		val superclasses = clsType.baseClasses
	  		// val mbrs = listType.members

			val containerTypeSuperClasses : Array[(String, ClassSymbol, Type)] = superclasses.map( mbr => {
				val clssym = mbr.fullName
				if (clssym == "scala.Any") {
					(clssym, null, null)
				} else {
					val cls = Class.forName(clssym)
					val symbol = mirror.classSymbol(cls)
					val typ = symbol.toType
					(clssym, symbol, typ)
				}
			}).toArray
			(nm, containerTypeSuperClasses)
	  	}).toArray
	  	
	  	var map : Map[String, Array[(String, ClassSymbol, Type)]] = Map[String,Array[(String, ClassSymbol, Type)]]()
	  	containersWithSuperClasses.foreach( pair => {
	  		val arg : String = pair._1
	  		val containerTypeSuperClasses : Array[(String, ClassSymbol, Type)] = pair._2
	  		map += (arg -> containerTypeSuperClasses)
	  	})
	  	map
	}

	
	def typeStringForSimple(node : PmmlExecNode, expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {
	  val typestring : Array[(String,Boolean,BaseTypeDef)] = node match {
	    case c : xConstant => constantKeyForSimpleNode(node.asInstanceOf[xConstant], expandCompoundFieldTypes)
	    case f : xFieldRef => fldRefArgKey(node.asInstanceOf[xFieldRef], expandCompoundFieldTypes)
	    case a : xApply => {
	    	val (fcnRetType, funcdef, funcArgs) = fcnArgKey(node.asInstanceOf[xApply])
	    	Array[(String,Boolean,BaseTypeDef)]((fcnRetType, false, funcdef.retType))
	    }
	    case _ => {
	    	ctx.logger.error("This kind of function argument is currently not supported") 
	    	Array[(String,Boolean,BaseTypeDef)](("None",false,null))
	    }
	  }
	  typestring
	}

	/** 
	 *  Discover the typestring for the supplied constant that is an argument for 
	 *  a simple function (i.e., not Iterable).  
	 */
	def constantKeyForSimpleNode(constNode : xConstant, expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {
		val typestring : Array[(String,Boolean,BaseTypeDef)] = constNode.dataType match {
			case "ident" => {
				ctx.getFieldType(constNode.Value.toString, expandCompoundFieldTypes)
			}
			case "fIdent" => {
				ctx.logger.error("During function match... usage of 'fIdent' type arguments in simple functions are not currently supported. ")
				ctx.logger.error("Their use is for the 'iterable' class functions.")
				Array[(String,Boolean,BaseTypeDef)](("Any",false,null))
			}
			case _ => {
				val scalaType : String = PmmlTypes.scalaDataType(constNode.dataType)
				val (typestr, typedef) : (String, BaseTypeDef) = ctx.MetadataHelper.getType(constNode.dataType)
				if (typedef == null) {
					ctx.logger.error("Unable to find a constant's type for apply function argument ... fcn = '${node.function} ") 
				}
				val isContainerWithFieldNames : Boolean = ctx.MetadataHelper.isContainerWithFieldOrKeyNames(typedef)
				Array[(String,Boolean,BaseTypeDef)]((typestr, isContainerWithFieldNames, typedef))
			}
		}
	  	typestring
	}
	
	/** 
	 *  Discover the typestring for the supplied FieldRef that is an argument for 
	 *  a simple function (i.e., not Iterable).  
	 */
	def fldRefArgKey(fldNode : xFieldRef, expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {
		val typestring : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(fldNode.field, expandCompoundFieldTypes)
		typestring
	}

	/** 
	 *  Discover the typestring for the supplied Apply that is an argument for 
	 *  a simple function (i.e., not Iterable).  NOTE: The function to be interpreted
	 *  does not necessarily have to be "simple" however.
	 */
	def fcnArgKey(fcnNode : xApply) : (String, FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = {
	  	val fcnNodeSelector : FunctionSelect = new FunctionSelect(ctx, mgr, fcnNode)
		val (funcDef, argTypes) : (FunctionDef, Array[(String,Boolean,BaseTypeDef)]) = fcnNodeSelector.selectFunction
		val typestring : String = if (funcDef != null) {
			funcDef.returnTypeString
		} else {
			"Any"
		}
		(typestring, funcDef, argTypes)
	}

	
	/** 
	 *  If the straight forward search key produced does not produce a FunctionDef, this more
	 *  elaborate mechanism is used to try to relax the argument types.
	 *  NOTE: This is a work in progress. First implementation will look for scalar arguments
	 *  of different widths and relax the width iff there are functions with a wider width
	 *  than was proposed in the first key produced.
	 */
	def relaxIterableKey(elementKey : Boolean) : String = {
	  	/** FIXME: provide an implementation for this */
		""
	}
	
	def typeStringForIterable(node : PmmlExecNode, expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {
	  val typestring : Array[(String,Boolean,BaseTypeDef)] = node match {
	    case c : xConstant => Array(constantKeyForIterableNode(node.asInstanceOf[xConstant]))
	    case f : xFieldRef => fldRefArgKey(node.asInstanceOf[xFieldRef], expandCompoundFieldTypes)
	    case a : xApply => {
	    	val (fcnRetType, funcdef, funcArgs) = fcnArgKey(node.asInstanceOf[xApply])
	    	Array((fcnRetType, false, funcdef.retType))
	    }
	    case _ => {
	    	ctx.logger.error("This kind of function argument is currently not supported") 
	    	Array[(String,Boolean,BaseTypeDef)](("None",false,null))
	    }
	  }
	  typestring
	}

	/** 
	 *  Discover the typestring for the supplied constant that is an argument for 
	 *  a simple function (i.e., not Iterable).  
	 */
	def constantKeyForIterableNode(constNode : xConstant) : (String,Boolean,BaseTypeDef) = {
		val typestring : (String,Boolean,BaseTypeDef) = constNode.dataType match {
		  	/** 
		  	 *  FIXME:
		  	 *  FIXME:
		  	 *  FIXME:
		  	 *  FIXME:
		  	 *  FIXME:
		  	 *  FIXME:
		  	 *  
		  	 *  FIXME:  For the ident types, the first child is a container that is searched for an attribute or container that has
		  	 *  the ident element in it.  For the fIdent types, this is the name of a function that should be sought in the 
		  	 *  metadata manager... another instance of this class will be tagged with finding a function def for the element function
		  	 *  with this name.  Note that any arguments after the fIdent are all arguments for the element function
		  	 */
			case "ident" => ("Any",false,null) /** fix this when implemented .. an ident could very well be a container */
			case "fIdent" => ("Any",false,null)
			case _ => {
				val scalaType : String = PmmlTypes.scalaDataType(constNode.dataType)
				val (argType,isContainer,elem) : (String,Boolean,BaseTypeDef) = ctx.getFieldType(MdMgr.SysNS,scalaType)
				(argType,isContainer,elem)
			}   
		}
	  	typestring
	}

}
