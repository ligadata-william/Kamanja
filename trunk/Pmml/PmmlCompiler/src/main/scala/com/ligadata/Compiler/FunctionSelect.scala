package com.ligadata.Compiler

import scala.collection.mutable._
import scala.collection.immutable.{ Set }
import scala.math._
import scala.collection.immutable.StringLike
import scala.util.control.Breaks._
import scala.reflect.runtime.universe._
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._
import com.ligadata.edifecs._

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

class FunctionSelect(val ctx : PmmlContext, val mgr : MdMgr, val node : xApply) extends LogTrait {

	/** Assess whether function has the ITERABLE feature.  The rule here is that if any function with this
	 *  name has the ITERABLE feature, they ALL MUST HAVE it.   */
	def isIterableFcn : Boolean = {
	  	var isIterable = false
	  	val (firstFcn,onlyOne) : (FunctionDef,Boolean) = representativeFcn(node.function)
	  	if (firstFcn != null) {
	  		isIterable = firstFcn.features.contains(FcnMacroAttr.ITERABLE)
	  	}
	  	isIterable
	}
  
	def selectSimpleFcn : FcnTypeInfo = {
	  	/** create a search key from the function name and its children (the arguments). */
	  	logger.trace(s"selectSimpleFcn ... search mdmgr for ${node.function}...")
	  	//var returnedArgs : Array[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)]
	  	//		= collectArgKeys()
	  	var argTypesExpanded : Array[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)] 
	  			= collectArgKeys(true)

	  	/** Project the various argTypeExpanded pieces into their own arrays */
	  	val argTypesExp : Array[Array[(String,Boolean,BaseTypeDef)]] = argTypesExpanded.map( tuple => tuple._1)
	  	val mbrFcnTypesExp : Array[Array[(String,Boolean,BaseTypeDef)]] = argTypesExpanded.map( tuple => tuple._2)
	  	val containerTypeDefs : Array[ContainerTypeDef] = argTypesExpanded.map( tuple => tuple._3)
	  	val memberTypes : Array[Array[BaseTypeDef]] = argTypesExpanded.map( tuple => tuple._4)
	  	val returnTypes : Array[String] = argTypesExpanded.map( tuple => tuple._5)  	
	  	
	  	val argTypes : Array[(String,Boolean,BaseTypeDef)] = argTypesExp.map( arg => arg.last)
	  	
	  	var simpleKey : String = buildSimpleKey(node.function, argTypes.map( argTriple => argTriple._1))
	  	val nmspcsSearched : String = ctx.NameSpaceSearchPath
	  	logger.trace(s"selectSimpleFcn ... key used for mdmgr search = '$nmspcsSearched.$simpleKey'...")
	  	//simpleKey = "Get(EnvContext,String,Long)"
	  	var funcDef : FunctionDef = ctx.MetadataHelper.FunctionByTypeSig(simpleKey)
	  	var winningKey : String = null
	  	val typeInfo : FcnTypeInfo = if (funcDef == null) {
	  		val simpleKeysToTry : Array[String] = relaxSimpleKey(node.function, argTypes, returnTypes)
	  		breakable {
	  		  	simpleKeysToTry.foreach( key => {
	  		  		logger.trace(s"selectSimpleFcn ...searching mdmgr with a relaxed key ... $key")
	  		  		funcDef = ctx.MetadataHelper.FunctionByTypeSig(key)
	  		  		if (funcDef != null) {
	  		  			logger.trace(s"selectSimpleFcn ...found funcDef with $key")
	  		  			winningKey = key
	  		  			break
	  		  		}
	  		  	})	  		  
	  		}
	  		if (funcDef != null) {
	  			new FcnTypeInfo(funcDef, argTypes, argTypesExp, winningKey)
	  		} else {
	  			null
	  		}
	  	} else {
	  		winningKey = simpleKey
	  		new FcnTypeInfo(funcDef, argTypes, argTypesExp, winningKey)
	  	}
	  	val foundDef : String = if (typeInfo != null)  "YES" else "NO!!"
	  	logger.trace(s"selectSimpleFcn ...funcDef produced? $foundDef ")
	  	typeInfo
	}
	
	/** 
	 *  Note: When the functions with nmspc.name has the ITERABLE feature, there are two searches
	 *  performed:
	 *  	1) for the collection function 
	 *   	2) for the element function that operates on the container elements
	 *    
	 */
	def selectIterableFcn : FcnTypeInfo = {
	  	var iterableFcn : FunctionDef = null
	  	var returnedArgs : 	Array[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)] =  collectIterableArgKeys(true)
	  	/** The 'returnedArgs' contains information for all arguments of the current apply function args
	  	 *  Since this is an iterable function, the first argument should be the collection (that possesses this Iterable trait) with
	  	 *  the remaining arguments describing the member function and any arguments that it may take.  Split the array into its
	  	 *  two parts ... the iterable argument and the member function arguments and project just the argument portions for
	  	 *  our purpose here
	  	 */ 
	  	val hasFIdent : Boolean = checkForMemberFcn
	  	val reasonableArgMinimum : Int = if (hasFIdent) 2 else 1 
	  	if (returnedArgs.size < reasonableArgMinimum) {
	  		logger.error("collectIterableArgKeys returned the wrong number of type info arrays... there should be 1 for iterable and one for the member function")
	  	}
	  	 	
	  	/** Project the various returnedArgs pieces into their own arrays */
	  	val argTypesExp : Array[Array[(String,Boolean,BaseTypeDef)]] = returnedArgs.map( tuple => tuple._1)
	  	var elemFcnArgs : Array[Array[(String,Boolean,BaseTypeDef)]] = returnedArgs.map( tuple => tuple._2)
	  	val containerTypeDefs : Array[ContainerTypeDef] = returnedArgs.map( tuple => tuple._3)
	  	val memberTypes : Array[Array[BaseTypeDef]] = returnedArgs.map( tuple => tuple._4)
	  	val returnTypes : Array[String] = returnedArgs.map( tuple => tuple._5)  	

	  	/** Get the simple argument for the receiver with the Iterable trait behavior */
	  	val iterableFcnArgs : Array[(String,Boolean,BaseTypeDef)] = Array[(String,Boolean,BaseTypeDef)](returnedArgs.head._1.last)
	  	
	  	val collectionType : ContainerTypeDef = returnedArgs.head._3
	  	val collectionsElementTypes : Array[BaseTypeDef] = returnedArgs.head._4
	 		  	
  		val fcnInfo = elemFcnArgs.tail.head
  		/** When an fIdent is present, the name will be non-null... when null there is a map doing a projection or something similar */ 
  		var elemFcnArgRange : (Int,Int) = (1,elemFcnArgs.size - 1) /** remember the child indices for the printer */
  		val elemFcnName : String = if (hasFIdent) {
  			elemFcnArgRange = (elemFcnArgRange._1 + 1, elemFcnArgRange._2)
  			val fcnInfoFull : Array[(String,Boolean,BaseTypeDef)] = elemFcnArgs.tail.head /** trim off the iterable collection that is null and grab the first type which should be the fIdent */
  			val fcnInfoFullSize : Int = fcnInfoFull.size
  			if (fcnInfoFullSize != 1) {
  				logger.error("collectIterableArgKeys member function has compound name... either bad mbr function name or not an fIdent in this position")
  			}
  			val fcnInfo : (String,Boolean,BaseTypeDef) = fcnInfoFull.head 
	  		val (fnm, _, _) = fcnInfo
	  		elemFcnArgs = elemFcnArgs.tail.tail /** clip the iterable (a null) and the function name from elem args to give true mbr fcn args */
	  		fnm
  		} else {
  			null
  		}
	  	
	  	val leafElemArgs : Array[(String,Boolean,BaseTypeDef)] = elemFcnArgs.map(fullArgs => {if (fullArgs != null) fullArgs.last else null})
	  	var elemFKey : String  = if (elemFcnName != null) buildIterableKey(true, elemFcnName, leafElemArgs, collectionType, collectionsElementTypes) else null
	  	var iterableFKey : String  = buildIterableKey(false, node.function, iterableFcnArgs, collectionType, collectionsElementTypes)
	  	
	  	var winningMbrKey : String = null
	  	var elementFcn : FunctionDef = null
	  	val fcnNamePresentInArgs : Boolean = (elemFcnName != null) 
	  	if (fcnNamePresentInArgs) {
	  		elementFcn = ctx.MetadataHelper.FunctionByTypeSig(elemFKey)
	  	}
	  	if (elementFcn == null && fcnNamePresentInArgs) {

	  		val doElemKey : Boolean = true
	  		val iterableFcnName : String = node.function
		  	val addlElemKeys : Array[String] = relaxIterableKeys(doElemKey
		  														, iterableFcnName
																, iterableFcnArgs
																, elemFcnName
																, leafElemArgs
																, collectionType
																, collectionsElementTypes
																, returnTypes)
		  	breakable {
	  		  	addlElemKeys.foreach(key => {
	  		  		logger.trace(s"selectIterableFcn ...searching mdmgr for mbr fcn with a relaxed key ... $key")
	  		  		elementFcn = ctx.MetadataHelper.FunctionByTypeSig(key)
	  		  		if (elementFcn != null) {
	  		  			winningMbrKey = key
	  		  			break
	  		  		}
	  		  	})
	  		  	if (addlElemKeys.size == 0){
	  		  		logger.trace("selectIterableFcn ...there were no keys to search for mbr function... must be fIdent-less mbr to be mapped/tupled")
	  		  	}
	  		}
	  	}
	  	
	  	/** Perform the iterable function lookup in any event.  It is currently acceptable
	  	 *  to have no element function.  A map function that does a simple projection of 
	  	 *  container fields is an example
	  	 */
	  	var winningKey : String = null
  		iterableFcn = ctx.MetadataHelper.FunctionByTypeSig(iterableFKey)
  		val typeInfo : FcnTypeInfo = if (iterableFcn == null) {
  			/** redo the Iterable fcn key too (chg container type to its parent or Any) */
  			val doElemKey : Boolean = false
	  		val iterableFcnName : String = node.function
  			val addlIterableKeys : Array[String] = relaxIterableKeys(doElemKey
  																	, iterableFcnName
  																	, iterableFcnArgs
  																	, elemFcnName
  																	, leafElemArgs
  																	, collectionType
  																	, collectionsElementTypes
  																	, returnTypes)
 		  	breakable {
	  		  	addlIterableKeys.foreach(key => {
	  		  		logger.trace(s"selectIterableFcn ...searching mdmgr for iterarble fcn with a relaxed key ... $key")
	  		  		iterableFcn = ctx.MetadataHelper.FunctionByTypeSig(key)
	  		  		if (iterableFcn != null) {
	  		  			winningKey = key
	  		  			break
	  		  		}
	  		  	})
	  		}
  			
  			/** 
  			 *  Record information that will be used by the printer to print the right phrase for the function invocation
  			 */
			
 			new FcnTypeInfo(iterableFcn
 							, iterableFcnArgs
 							, argTypesExp
 							, winningKey
 							, elementFcn
 							, leafElemArgs
 							, elemFcnArgs 
 							, elemFcnArgRange
 							, collectionType
 							, collectionsElementTypes
 							, winningMbrKey
 							, returnTypes)
  		} else {
  			new FcnTypeInfo(iterableFcn
  							, iterableFcnArgs
  							, argTypesExp
  							, winningKey
  							, elementFcn
  							, leafElemArgs
  							, elemFcnArgs
  							, elemFcnArgRange
  							, collectionType
  							, collectionsElementTypes
  							, winningMbrKey
  							, returnTypes)
  		}
  		
	  	typeInfo
	} 
	
	/** 
	 *  Check if the current iterable function has a member function (most do).  If not, the iterable function
	 *  is doing a projection on one or more of some container of fields.
	 */
	def checkForMemberFcn : Boolean = {
		var hasFcn : Boolean = false
		var fcnName : String = null
		var cnt : Int = 0
		breakable {
		  	node.Children.foreach( child => {
		  		cnt += 1
		  		if (child.isInstanceOf[xConstant]) {
		  			val constChild : xConstant = child.asInstanceOf[xConstant]
		  			if (constChild.dataType.toLowerCase() == "fident") {
		  				fcnName = constChild.Value.toString
		  				hasFcn = true
		  				break
		  			}
		  		}
		  	})
		  	if (hasFcn && cnt != 2) {
		  		logger.warn(s"The member function $fcnName for iterable function ${node.function} is not in the 2nd position")
		  	}
		}
		hasFcn
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
	
	/** Collect the child arguments for simple function. */
	def collectArgKeys(expandCompoundFieldTypes : Boolean = false) : 
		Array[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)]= {

	  	val noChildren : Int = node.Children.size
	  	var argTypes : ArrayBuffer[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)] 
	  				= ArrayBuffer[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)]()
	  	node.Children.foreach ( child => {
	  		val childArgs
  					: (Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)
  					= typeStringFor(child, expandCompoundFieldTypes)
  			argTypes += childArgs
  		})
	  	argTypes.toArray
	}
	
	/** Collect the child arguments for an iterable function. These functions are typically comprised of two functions
	 *  the collection function (e.g., filter, map, etc.) and the element function (a function that operates on 
	 *  one or more fields of the collection specified in arg 1.
	 * 
	 *  As a consequence, there are two argType triples returned, one for the outer function (map, filter, etc.) and
	 *  one for the inner function that operates on the iterable function.
	 *  
	 *  As a consequence, the first array element in the array returned will have just one type triple - namely for the 
	 *  iterable collection reference.  The second element will have type triples for the remaining pmml arguments to this
	 *  iterable function.  The fIdent element, if present is skipped.  It will be located and used by the key preparation 
	 *  function for iterables.
	 */
	def collectIterableArgKeys(expandCompoundFieldTypes : Boolean = false) : 
			Array[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)] = {

	  	val buffer : StringBuilder = new StringBuilder
	  	val noChildren : Int = node.Children.size
	  	var collectionsElementTypes : Array[BaseTypeDef] = null
	  	var containerType : ContainerTypeDef = null
	  	var cnt : Int = 0
	  	var returnVals : ArrayBuffer[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)] =
	  	  	ArrayBuffer[(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String)]()

	  	node.Children.foreach ( child => {
	  		cnt += 1
	  		if (cnt == 1) {
	  			if (child.isInstanceOf[xFieldRef] && child.asInstanceOf[xFieldRef].field == "AllInPatientDiagnoses") {
	  				val wtf : String = "wtf"
	  			}
	  			/** get the collection type overall */
	  			val (iterArgs, mbrArgs, collType, collArgTypes, retTypStr) : 
	  				(Array[(String,Boolean,BaseTypeDef)], Array[(String,Boolean,BaseTypeDef)], ContainerTypeDef, Array[BaseTypeDef], String)
	  					= typeStringFor(child, expandCompoundFieldTypes)
	  			if (iterArgs != null) {
		  			/** Remember the item types for the collection. NOTE: there could be multiple for tuples and maps 
		  			 *  NOTE: The collType and collArgTypes are null in first arg .. at least for current code version.  The current system
		  			 *  expects a field reference in the first position that is to contain the collection.  This may
		  			 *  be relaxed when the type inference is more robust. We could simply check for the collType to be non null and use it
		  			 *  instead of this method below. */
		  			val iterableType : BaseTypeDef = iterArgs.last._3
		  			containerType = 
		  			  		if (iterableType.isInstanceOf[ContainerTypeDef]) { 
		  						iterableType.asInstanceOf[ContainerTypeDef] 
		  			  		} else {
		  			  			null
		  			  		}
		  			if (containerType != null) {
		  				collectionsElementTypes = containerType.ElementTypes
		  				val containerTypeTypeStr : String = ObjType.asString(containerType.tType)
		  				buffer.clear
		  				buffer.append("[")
		  				if (collectionsElementTypes.size > 0) {
		  					collectionsElementTypes.foreach( mbrType => buffer.append(s" ${mbrType.typeString} "))
		  				} else {
		  					buffer.append("NO ELEMENTS")
		  				}
		  				buffer.append("]")
		  				val collectionElementsTypeStr : String = buffer.toString
		  				logger.trace(s"Container ${containerType.FullName} (a $containerTypeTypeStr) has element types: $collectionElementsTypeStr")
		  			}
	  				  			
	  				val returnStuff = (iterArgs,mbrArgs,containerType,collectionsElementTypes,retTypStr)
	  				returnVals += returnStuff
	  			}
	  			
	  		} else {
	  			val fcnArgs : Array[(String,Boolean,BaseTypeDef)] = 
	  				  	typeDefsForIterablesFunction(child, expandCompoundFieldTypes, containerType, collectionsElementTypes)	
	  			/** The mbr fcn args, containerType, and collectionElementTypes can be set to null only because nested 
	  			 *  iterable functions are not supported.
	  			 */
	  			val returnStuff = (null, fcnArgs, null, null, null)
	  			returnVals += returnStuff
	  			
	  		}
	  				
  		})
	  	returnVals.toArray
	}
	
	
	
	/** Create a type string from the key, sans namespace 
	 * 
	 *  @param fcnName - the name of the function
	 *  @param argtypes - the arg type info collected for the apply function 
	 *  @return a search key to be used to find the function in the metadata.
	 */
	def buildSimpleKey(fcnName : String, argtypes : Array[String]) : String = {
		val keyBuff : StringBuilder = new StringBuilder()
	  	keyBuff.append(fcnName)
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
	
	/** 
	 *  One or more functions with node.function as name exists and has been
	 *  determined to be an "Iterable" function.  This means that any of the 
	 *  functions with this name must be "Iterable" and as such we build two 
	 *  keys for both the element function and the Iterable function that uses
	 *  it.  Supply the correct boolean value to choose which should be built. 
	 *  
	 *  When building element function key, there can be two cases.  If there is no
	 *  fIdent then the remaining arguments are assumed to be either constants or 
	 *  field identifiers for the Iterable collection item (first arg).  This is 
	 *  appropriate only for the 'map' function.  If more than one element a tuple 
	 *  will be formed for the elements in the generated code.
	 *  
	 *  When fIdent is present, the remaining arguments are considered to be arguments for
	 *  this function.  If the function 'fIdent'ified was 'Between' it might look like
	 *  this:
	 *  
	 *  	Between(Int,Int,Int,true)
	 *   
	 *  The types for the idents and/or constants (from their baseType) are used to fill
	 *  the list.  The idents will be a field reference in the Iterable found in arg 1. 
	 *  
	 *  When the element function key is not being formed, the Iterable collection key 
	 *  is being formed.  For example, a ContainerFilter function search key whose 
	 *  first argument (the Iterable) is an Array, would have a key like this:
	 *  
	 *  	ContainerFilter(scala.collection.mutable.Array[Any]
	 *  
	 *  @param buildElementFcnKey - when true the element function key is built else the primary function key
	 *  @param argTypes - the argument type info for all elements specified in the pmml apply arg list.
	 *  @param containerType - the type info for the iterable's collection
	 *  @param collectionsElementTypes - the member(s) type info for the iterable's collection
	 *  @return a search key to be used to search for the function in the metadata
	 */
	def buildIterableKey(buildElementFcnKey : Boolean
						, fcnName : String
					    , argTypes: Array[(String,Boolean,BaseTypeDef)]
						, containerType: ContainerTypeDef
						, collectionsElementTypes : Array[BaseTypeDef]) : String = {
		val keyBuff : StringBuilder = new StringBuilder()
	  	if (buildElementFcnKey) {
	  		if (fcnName != null) {  /** could be a simple projection with map ... i.e., no fcn */
		  		val args : Array[(String,Boolean,BaseTypeDef)] = argTypes.tail
			  	keyBuff.append(fcnName)
				keyBuff.append('(')
			  	var cnt : Int = 0
			  	argTypes.foreach (arg => {
			  		val (typeStr, isContainer, baseType) : (String,Boolean,BaseTypeDef) = arg
			  		keyBuff.append(typeStr)
			  		cnt += 1
			  		if (cnt < argTypes.size) keyBuff.append(',')
			  	})
			  	keyBuff.append(')')  
		  	}
	  	} else { /** do outer function ... only the function name and its collection in first pos */
	  		val nestLevel : Int = argTypes.size
	  		val fcnNm : String = node.function
	  		keyBuff.append(s"$fcnNm(")
	  		keyBuff.append(s"${containerType.typeString})")	  		
	  	}
	  	val simpleKey : String = keyBuff.toString
	  
	  	simpleKey
	}
	
	/** 
	 *  If the straight forward search key produced does not produce a FunctionDef, this more
	 *  elaborate mechanism is used to try to relax the argument types. Answer an array of 
	 *  additional keys to utilize for function type search.
	 *  
	 *  NOTE: This is a work in progress. 
	 *  
	 *  Current method:
	 *  	relaxation 1) 
	 *   			a) If there are collections in the argument list, collect the superclasses
	 *   			for each of their member types that are containers, if any.  Promote their types 
	 *      		to the first abstract class or trait found in their respective superclass list. 
	 *        		Form a new type with the original outer collection.  Non container types remain
	 *          	unchanged.
	 *           	b) Change the member types to Any keeping the outer collection the same.
	 *            	c) If the returnTypes array has a type or types in it, use it as the member types
	 *  	relaxation 2) If there are containers in the argument list, collect the superclasses
	 *   			for each of them, find the first abstract or trait and use it
	 *      relaxation 3) Change containers to Any
	 *      relaxation 4) If there are scalars, broaden their width
	 *      relaxation 5) Make all arguments "Any"
	 *      
	 *  @param fcnName the function' name
	 *  @param argTypes the function's argument type info
	 *  @param returnTypes if there are any valid type string(s) in it, use them to substitute for the member type(s).  These "return types"
	 *  	are extracted from an argument that uses a map (or similar) function in it that changes the types of the receiver array.  The 
	 *   	'map' function is common as it is used to operate on the receiver array content (e.g., summing a number of fields in an array 
	 *    	of structures) and therefore changes array's member type to the member function's return type.
	 *  @return an array of search keys to try that have been configured by one of the relaxation strategies.
	 */

	def relaxSimpleKey(fcnName : String, argTypes : Array[(String,Boolean,BaseTypeDef)], returnTypes : Array[String]) : Array[String] = {

	  	var relaxedKeys : ArrayBuffer[String] = ArrayBuffer[String]()
	  	
	  	/** 1.a */
	  	/** 
	  	 *  For any container type defs that have member types, promote these member types to the first trait or 
	  	 *  abstract class found in their respective superclasses.  The outer collection that has the changed
	  	 *  member type(s) is not changed. Non container arguments are left unchanged. In certain cases, it is possible
	  	 *  to have no candidates returned... hence the guard.
	  	 */
	  	val containerArgsWithPromotedMemberTypes : Array[(String,Boolean,BaseTypeDef)] = relaxCollectionMbrTypesToFirstTraitOrAbstractClass(argTypes)
	  	if (containerArgsWithPromotedMemberTypes != null && containerArgsWithPromotedMemberTypes.size > 0) {
		  	val relaxedTypes1a : Array[String] = containerArgsWithPromotedMemberTypes.map( argInfo => {
		  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
		  		arg
		  	})
		  	if (relaxedTypes1a != null && relaxedTypes1a.size > 0) {
		  		relaxedKeys += buildSimpleKey(fcnName, relaxedTypes1a)
		  	}
	  	}
	  	
	  	/** 1.b */
	  	/** 
	  	 *  Change the member types to Any keeping the outer collection the same.
	  	 */
	  	val containerArgsWithAnyMemberTypes : Array[(String,Boolean,BaseTypeDef)] = relaxCollectionMbrTypesToAny(argTypes)
	  	val relaxedTypes1b : Array[String] = containerArgsWithAnyMemberTypes.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		arg
	  	})
	  	if (relaxedTypes1b != null && relaxedTypes1b.size > 0) {
	  		relaxedKeys += buildSimpleKey(fcnName, relaxedTypes1b)
	  	}
	  	
	  	/** 1.c */
	  	/** 
	  	 *  If the returnTypes array has a type or types in it, use it as the member type for the corresponding argument.
	  	 *  If all returnTypes are null, don't bother.  
	  	 */
	  	val hasReturnKeys : Boolean = (returnTypes != null && returnTypes.filter(_ != null).size > 0)
	  	if (hasReturnKeys) {
		  	val relaxedTypes1c : Array[String] = relaxCollectionMbrTypesToReturnType(argTypes, returnTypes)
		  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes1c)
	  	}
	  	
	  	/** 1.d */
	  	/** 
	  	 *  If the returnTypes array has a type(s) in it, directly substitute the return type in corresponding position in the argtypes.
	  	 */
	  	if (hasReturnKeys) {
		  	val relaxedTypes1d : Array[String] = relaxTypesToReturnType(argTypes, returnTypes)
		  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes1d)
	  	}
	  	
	  	/** 1.e */
	  	/** 
	  	 *  If the returnTypes array has a type or types in it AND it is an iterable container with perhaps a too specific 
	  	 *  member type(s), substitute 'Any' as the member type or types.
	  	 */
	  	if (hasReturnKeys) {
		  	val relaxedTypes1e : Array[String] = relaxReturnedIterableTypeMembers(argTypes, returnTypes)
		  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes1e)
	  	}
	  		  	
	  	/** 2. */
	  	/** 
	  	 *  Change the containers in the argTypes to use the first base class they have that is either an abstract
	  	 *  class or trait 
	  	 */
	  	
	  	val argsWithPromotedContainerClasses : Array[(String,Boolean,BaseTypeDef)] = relaxToFirstTraitOrAbstractClass(argTypes)
	  	val relaxedTypes2 : Array[String] = argsWithPromotedContainerClasses.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		arg
	  	})
	  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes2)
	  		  	
	  	/** 3. */
	  	val relaxedTypes3 : Array[String] = argTypes.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		if (isContainer) {
	  			"Any"
	  		} else {
	  			arg
	  		}
	  	})
	  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes3)
	  		  	
	  	/** 4. */
	  	val relaxedTypes4 : Array[String] = argTypes.map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		arg match {
	  		  case "Int" => "Long"
	  		  case "Float" => "Double"
	  		  case _ => arg
	  		}
	  	})
	  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes4)
	  	
	  	/** 5. */
	  	val relaxedTypes5 : Array[String] = argTypes.map( argInfo => {
	  		"Any"
	  	})
	  	relaxedKeys += buildSimpleKey(fcnName, relaxedTypes5)
	  	
	  	logger.trace("...relaxed keys:")
	  	relaxedKeys.foreach( key => logger.trace(s"\t$key"))
	  
	  	relaxedKeys.toArray
	}
	
	
	/** 
	 *  Prepare a new set of argTypes that promote the element types of those ContainerTypeDefs in the argTypes array that 
	 *  have them to their respective first trait or abstract class found in its list of superclasses (@see collectCollectionElementSuperClasses).
	 *  The Collection container for these ContainerTypeDefs remains unchanged.
	 *  
	 *  @param argTypes - the argument type info for a given apply function
	 *  @return an array of argument types with the container elements' types possibly promoted to their abstract trait.
	 */
	def relaxCollectionMbrTypesToFirstTraitOrAbstractClass(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Array[(String,Boolean,BaseTypeDef)] = { 
	  	 
	  	val collectionMbrTypes : Map[String, Array[List[(String, ClassSymbol, Type)]]] = collectCollectionElementSuperClasses(argTypes)  
	  	val modifiedArgTypes : Array[(String,Boolean,BaseTypeDef)] = if (collectionMbrTypes != null && collectionMbrTypes.size > 0) {
		  	val buffer : StringBuilder = new StringBuilder
		  	val newArgTypes : Array[(String,Boolean,BaseTypeDef)] = argTypes.map( argType => {
		  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argType
		  		val newMbrTypes : ArrayBuffer[String] = if (isContainer) {
		  			val argTypeInfo = collectionMbrTypes(arg)
					var newTypes : ArrayBuffer[String] = new ArrayBuffer[String]()
			  		breakable {
			  			argTypeInfo.foreach( mbrArgTypeInfos => {
			  				val superClsTypes : List[(String, ClassSymbol, Type)] = mbrArgTypeInfos
			  				superClsTypes.foreach( mbrSupTypeCandidate => {
			  					val (clssym, symbol, typ) : (String, ClassSymbol, Type) = mbrSupTypeCandidate
				  				
				  				if (symbol != null && (symbol.isAbstractClass || symbol.isTrait)) {
				  					newTypes += symbol.fullName
				  					break
				  				}	  				  		  				  
			  				})
			  			})
			  			break
			  		}
		  			newTypes
		  		} else {
		  			null
		  		}
		  		
		  		/** Use the newMbrTypes to form a new version of this container's type */
		  		val typeStr : String = if (newMbrTypes != null && newMbrTypes.size > 0) {
		  			val collectionPart : String = arg.split('[').head
		  			buffer.clear
		  			newMbrTypes.addString(buffer, ",")
		  			val elementTypes : String = buffer.toString
		  			buffer.clear
		  			buffer.append(s"$collectionPart[$elementTypes]")
		  			buffer.toString
		  		} else {
		  			arg
		  		}
		  		val useThisTuple : (String, Boolean, BaseTypeDef) = (typeStr, isContainer, elem)
		  		useThisTuple
		  	})
		  	newArgTypes
		} else {
		  	Array[(String, Boolean, BaseTypeDef)]()
		}

	  	modifiedArgTypes.toArray
	}
	
	/** 
	 *  For ContainerTypeDefs that have an element or element (e.g., Array[SomeBigStructure] that is a container type,
	 *  change it to 'Any' (i.e., 'SomeBigStructure' to 'Any'), keeping the base collection the same (in the example
	 *  'Array').
	 *  
	 *  @param argTypes a triple of type information that describes the arguments to the function being considered 
	 *  	(type string, isContainerWithFields, the arg metadata)
	 *  @return a new set of arguments with the described transformations made
	 */
	
	def relaxCollectionMbrTypesToAny(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Array[(String,Boolean,BaseTypeDef)] = {

	  	val buffer : StringBuilder = new StringBuilder
	  	val newArgInfo : Array[(String,Boolean,BaseTypeDef)] = argTypes.map ( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		
	  		val newArg : String = if (elem.isInstanceOf[ContainerTypeDef] && elem.asInstanceOf[ContainerTypeDef].ElementTypes.size > 0) {
	  			val coll : ContainerTypeDef = elem.asInstanceOf[ContainerTypeDef]
	  			val collectionPart : String = arg.split('[').head
	  			buffer.clear
	  			val newElements : Array[String] = coll.ElementTypes.map( e => "Any" )
	  			buffer.clear
	  			newElements.addString(buffer, ",")
	  			val elementTypePart : String = buffer.toString
	  			buffer.clear
	  			buffer.append(s"$collectionPart[$elementTypePart]")
	  			buffer.toString
	  		} else {
	  			arg
	  		}
	  		
	  		(newArg, isContainer, elem)
	  	})

		newArgInfo	
	}
	/** 
	 *  When a function has an argument that is an iterable function and that argument was a 'map' function or something 
	 *  similar, the receiver array that was used has its type changed to whatever the member function in the map
	 *  returns.  This return type is captured and included in the returnTypes array.  If there is at least one 
	 *  such valid return type in the return types array, this function is called and a new set of arguments is generated
	 *  such that the target argument (a collection) has its element type(s) changed to the corresponding return type.
	 *  
	 *  Valid values are only populated for these iterable function arguments where type transformation takes place.
	 *  All other cases are set to null
	 *  
	 *  @param argTypes a triple of type information that describes the arguments to the function being considered 
	 *  	(type string, isContainerWithFields, the arg metadata)
	 *  @param returnTypes when a 'map' function is present the current function's arguments at some position, this 
	 *  	array will have the correct type for that argument.  Substitute it.
	 *  @return a new arguments with the described transformations made
	 */
	
	def relaxCollectionMbrTypesToReturnType(argTypes : Array[(String,Boolean,BaseTypeDef)], returnTypes : Array[String]) 
	  		: Array[String] = {

	  	val buffer : StringBuilder = new StringBuilder
	  	
	  	var idx : Int = 0
	  	if (returnTypes.size != argTypes.size) {
	  		logger.error("Inappropriate returnTypes array supplied here... investigate")
	  	}
	  	val newArgInfo : Array[String] = argTypes.map ( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		
	  		val returnType = returnTypes.apply(idx)
	  		val newArg : String = if (elem.isInstanceOf[ContainerTypeDef] 
	  								&& elem.asInstanceOf[ContainerTypeDef].ElementTypes.size > 0 
	  								&& returnType != null) {
	  			val coll : ContainerTypeDef = elem.asInstanceOf[ContainerTypeDef]
	  			val collectionPart : String = arg.split('[').head
	  			buffer.clear
	  			/** FIXME: This code will need tending when Map and Tuples are supported here. I believe
	  			 *  that there should be tuples in the returnTypes array for those */
	  			val rTypeParts : Array[String] = returnType.split('[')
	  			val returnTypeElementType : String = if (rTypeParts.size > 1) {
	  				/** get the element type(s) in the brackets */
	  				encloseElementArgs(returnType, '[' , ']')
	  			} else {
	  				null
	  			}
	  			
	  			if (returnTypeElementType != null && returnTypeElementType.size > 0) {	
	  				val newElements : Array[String] = coll.ElementTypes.map( e => returnTypeElementType )
		  			buffer.clear
		  			newElements.addString(buffer, ",")
		  			val elementTypePart : String = buffer.toString
		  			buffer.clear
		  			buffer.append(s"$collectionPart[$elementTypePart]")
		  			buffer.toString
	  			} else {
	  				arg
	  			}
	  		} else {
	  			arg
	  		}
	  		idx += 1
	  		
	  		newArg
	  	})

		newArgInfo	
	}
	
	
	/** 
	 *  Substitute the return types collected (there is a return type for any function argument) for the 
	 *  corresponding argument type. 
	 *  
	 *  @param argTypes a triple of type information that describes the arguments to the function being considered 
	 *  	(type string, isContainerWithFields, the arg metadata)
	 *  @param returnTypes a function is present the current function's arguments at some position, this 
	 *  	type string will be sustituted in _._1 of the argTypes array.
	 *  @return a new set of arguments with the described transformations made
	 */
	
	def relaxTypesToReturnType(argTypes : Array[(String,Boolean,BaseTypeDef)], returnTypes : Array[String]) 
	  		: Array[String] = {
	  	
	  	var idx : Int = 0
	  	if (returnTypes.size != argTypes.size) {
	  		logger.error("Inappropriate returnTypes array supplied here... investigate")
	  	}
	  	val newArgInfo : Array[String] = argTypes.map ( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		
	  		val returnType = returnTypes.apply(idx)
	  		val newArg : String = if (returnType != null) returnType else arg
	  		idx += 1
	  		
	  		newArg
	  	})
		newArgInfo	
	}
	
	/** 
	 *  Similar to function relaxTypesToReturnType, this relaxation checks if the return type is also a 
	 *  an 'iterable' container of some sort that has member type(s) that are perhaps too specific.  Relax 
	 *  these "too" specific types by replacing them with 'Any'.
	 *  
	 *  @param argTypes a triple of type information that describes the arguments to the function being considered 
	 *  	(type string, isContainerWithFields, the arg metadata)
	 *  @param returnTypes when a 'map' function is present the current function's arguments at some position, this 
	 *  	array will have the correct type for that argument.  Substitute it.
	 *  @return a new arguments with the described transformations made
	 */
	
	def relaxReturnedIterableTypeMembers(argTypes : Array[(String,Boolean,BaseTypeDef)], returnTypes : Array[String]) : Array[String] = {

	  	val buffer : StringBuilder = new StringBuilder
	  	
	  	var idx : Int = 0
	  	if (returnTypes.size != argTypes.size) {
	  		logger.error("Inappropriate returnTypes array supplied here... investigate")
	  	}
	  	val newArgInfo : Array[String] = argTypes.map ( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		
	  		val returnType = returnTypes.apply(idx)
	  		val newArg : String = if (returnType != null) returnType else arg
	  		val relaxedNewArg : String = if (newArg != arg && newArg.contains("[")) {
	  			val collectionPart : String = newArg.split('[').head
	  			buffer.clear

	  			/** FIXME: Fix for nested collections and tuples... this currently only supports
	  			 *  single collection with one or more member types. */

	  			/** get the element type(s) in the brackets */
	  			val mbrType : String = encloseElementArgs(returnType, '[' , ']')

	  			val hasTuple : Boolean = (mbrType.contains("("))
	  			val hasNestedColl : Boolean = (mbrType.contains("["))
	  			if (hasTuple || hasNestedColl) {
	  				logger.warn(s"For function ${node.function}, nested collections and/or collections with tuple members are not supported by function lookup.")
	  				newArg
	  			} else {	  			
		  			if (mbrType != null && mbrType.size > 0) {	
		  				val mbrTypes : Array[String] = mbrType.split(',')
		  				val mbrTypeCnt : Int = mbrTypes.size
		  				val newMbrType : String = if (mbrTypeCnt > 1) {
		  					val newMbrTypes : Array[String] = mbrTypes.map(elmType => "Any")
		  					newMbrTypes.addString(buffer, ",")
		  					buffer.toString	  				  
		  				}	else {  				  
		  					"Any"
		  				}
			  			buffer.clear
			  			buffer.append(s"$collectionPart[$newMbrType]")
			  			buffer.toString
		  			} else {
		  				arg
		  			}
	  			}
	  		} else {
	  			arg
	  		}
	  		idx += 1
	  		
	  		relaxedNewArg
	  	})

		newArgInfo	
	}
	


	/** 
	 *  Extract the text between the two specified characters from the supplied string.  The characters supplied are 
	 *  assumed to be matching pairs.  This function handles nested bracketed types for example like this:
	 *  	Array[Array[StructureOfSomeKind]]
	 *  The function will return 'Array[StructureOfSomeKind]'
	 */
	def encloseElementArgs(typeString : String, openBracketOrParen : Char, closeBracketOrParen : Char) : String = {
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
	
	/** 
	 *  For ContainerTypeDefs that have an element or elements, determine which of them have container members.
	 *  Make an array of their superclasses similar to collectContainerSuperClasses.  Since there can be more
	 *  than one element type used to specify a collection, answer a map with an array of array triples as value - one
	 *  array for each collection element type.
	 *  
  	 *	Iterate over the container arguments.  If the container has ElementTypes, collect them.  For containers without
  	 *  element types, produce an empty array of empty array.  For each ContainerTypeDef that is an element of the 
  	 *  containers in 'collFullPkgNames' create the list of superclasses for each one, with (null,null,null) for those
  	 *  elements in the container elements that are not containers.
  	 *  
  	 *  FIXME: This is a recursive issue.  Conceivably, each of the elements could in turn be containers with element types of their
  	 *  own to an arbitrary depth.  For the current use cases, this is array of array of array... issue is not present.
  	 *  We will detect the nesting here and issue a warning message.  If models are presented with these sorts of types, the
  	 *  compile will fail. 
	 *
	 *  @param argTypes (the type (class) name for the container, if it is a container, the metadata for this type)
	 *  @return a map keyed by the arg typestring with value an array of a List of superclass info (type, class symbol, type symbol)  
	 *  	for each element container type.  If the element is not a container or there are no containers in the args supplied,
	 *   	answer nulls as needed
	 */
	
	def collectCollectionElementSuperClasses(argTypes : Array[(String,Boolean,BaseTypeDef)]) : Map[String, Array[List[(String, ClassSymbol, Type)]]] = {
	
	  	/** First get the arguments that are potentially collections with ElementTypes */
		val mirror = runtimeMirror(this.getClass.getClassLoader)
		/** only container types that have element types */
	  	val collFullPkgNames : Array[(String,String,ContainerTypeDef)] = 
	  	  		argTypes.filter(arg => { arg._3.isInstanceOf[ContainerTypeDef] && 
	  	  								 arg._3.asInstanceOf[ContainerTypeDef].ElementTypes.size > 0} ).map( argInfo => {
	  		val (arg, isContainer, elem) : (String, Boolean, BaseTypeDef) = argInfo
	  		(arg, elem.typeString, elem.asInstanceOf[ContainerTypeDef])
	  	})

		val collElementsWithSuperClasses : Array[(String,Array[List[(String, ClassSymbol, Type)]])] = collFullPkgNames.map( triple => {
	  		val (nm, fqClassname, containerElem) : (String, String, ContainerTypeDef) = triple
	  
	  		val containerElements : Array[BaseTypeDef] = containerElem.ElementTypes
	  		
	  		val elementTypeInfo : (String, Array[List[(String, ClassSymbol, Type)]]) = if (containerElements.size > 0) {
	  			var elementTypesDecorated : ArrayBuffer[List[(String, ClassSymbol, Type)]] = ArrayBuffer[List[(String, ClassSymbol, Type)]]()
	  			containerElements.foreach( mbrType => {
	  				val isMbrTypeAContainer : Boolean = mbrType.isInstanceOf[ContainerTypeDef]
	  				if (isMbrTypeAContainer) {
	  					/** Check for recursion ... we are not supporting mbr types who themselves have member types YET */
	  					val mbrContainer : ContainerTypeDef = mbrType.asInstanceOf[ContainerTypeDef]
	  					if (mbrContainer.ElementTypes.size > 0) {
	  						logger.warn("Collections of collections are not directly supported.  You must unwrap these nested collections by using Pmml variables.")
	  						logger.warn("Alternatively, use a container outside of the model to store your complex structures.. via the global context.")
	  					}
	  					val useThisName = mbrContainer.typeString
				  		val clz = Class.forName(useThisName)
						// Convert class into class symbol
						val clsSymbol = mirror.classSymbol(clz)
						// Info about the class
						val isTrait = clsSymbol.isTrait			 
						val isAbstractClass = clsSymbol.isAbstractClass
						val isModule = clsSymbol.isModule
						val subclasses : Set[reflect.runtime.universe.Symbol] = clsSymbol.knownDirectSubclasses 
						// Convert the class symbol into a Type
						val clsType = clsSymbol.toType
				  		val superclasses = clsType.baseClasses
				  		
				  		/** create the list of superclasses for this container member's type */
						val containerElementTypeSuperClasses : List[(String, ClassSymbol, Type)] = superclasses.map( mbr => {
							val clssym = mbr.fullName
							if (clssym == "scala.Any") {
								(clssym, null, null)
							} else {
								val cls = Class.forName(clssym)
								val symbol = mirror.classSymbol(cls)
								val typ = symbol.toType
								(clssym, symbol, typ)
							}
						})

						elementTypesDecorated += containerElementTypeSuperClasses
	  					
	  				} else {
	  					/** when not a container return a null for the non container mbr type  */
	  					elementTypesDecorated += List[(String,ClassSymbol,Type)]((mbrType.typeString, null, null)) 
	  				}
	  			  
	  			  
	  			})
	  			
	  			(nm, elementTypesDecorated.toArray)
	  			
	  		} else {
	  			/** if there are no containers at all, just return a null */
	  			(nm, null)
	  		}
	  		elementTypeInfo
	  	})

		val map : Map[String, Array[List[(String, ClassSymbol, Type)]]] = Map[String, Array[List[(String, ClassSymbol, Type)]]]()
	  	collElementsWithSuperClasses.foreach( pair => {
	  		val arg : String = pair._1
	  		val containerMbrTypeSuperClasses : Array[List[(String, ClassSymbol, Type)]] = pair._2
	  		map += (arg -> containerMbrTypeSuperClasses)
	  	})
	  	
	  	map
	}
	
 	
	/** Prepare a new set of argTypes that promote the container typedef to their respective abstract class
	 *  superclass or trait.  FIXME: Some types have multiple base types that could be used.  The current strategy
	 *  'break's on the first one.  This is not the best.  It should be made to stop on the first appropriate trait
	 *  for the class of type being examined (e.g., "Iterable" or "Traversable").  This could be implemented by 
	 *  mapping the plain function name to the traits that contain that method as an abstract function.
	 *  
	 *  @param argTypes - the argument type info for a given apply function
	 *  @return an array of argument types with the container types possibly promoted to their abstract trait.
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
	 *  
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
	  		//val ru = scala.reflect.runtime.universe
	  		//val clsz1 = ru.newTermName(fqClassname)
	  		//val cm = mirror.reflectClass(clsz1)
	  		//val classC = ru.typeOf[clsz1].typeSymbol.asClass
	  		val useThisName = if (fqClassname.contains("[")) fqClassname.split('[').head else fqClassname
	  		val clz = Class.forName(useThisName)
			// Convert class into class symbol
			val clsSymbol = mirror.classSymbol(clz)
			// Info about the class
			val isTrait = clsSymbol.isTrait			 
			val isAbstractClass = clsSymbol.isAbstractClass
			val isModule = clsSymbol.isModule
			val subclasses : Set[reflect.runtime.universe.Symbol] = clsSymbol.knownDirectSubclasses 
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

	/** 
	 *  Gather type information for the supplied argument node of some function. 
	 *  There are three valid cases.  The node is one of:
	 *  
	 *  <ul>
	 *  <li>An xConstant ... an ordinary constant,</li>
	 *  <li>An xFieldRef ... referring to some field in one of the dictionaries, messages or containers (possibly container qualified),</li>
	 *  <li>An xApply ... a function, possibly an iterable function.</li>
	 *  </ul>
	 *  
	 *  The function accounts for most of the complexity.  If it is an iterable function, it has two sets of arguments to pack
	 *  out of here to the caller.  All of these types are to be exposed to the printer and type inference tools
	 *  that used to correctly represent the generated output for the function under consideration.
	 *  
	 *  @param node - some PmmlExecNode that can appear in a function argument list
	 *  @param expandCompoundFieldTypes this flag causes all type information to be returned for the 
	 *  	container qualified field name references.
	 *  @return (the type info for the outer function, type info for the member function should if appropriate,
	 *  		the container type def for the iterable function's collection if appropriate, the 
	 *    		iterable function collection's element type(s) if appropriate, and the return type 
	 *      	for the function as needed).
	 *  When not a function, all but the first argument type info is null.  If the arg types have been requested to
	 *  be expanded, these arrays will have more than one member in them to reflect the container types enclosing
	 *  the leaf field.  When no type expansion is requested, just the leaf type info will be in the array.
	 *  
	 *  When functions, the iterable functions have two type info arrays filled.  Usually the first element of
	 *  the mbr function arguments will be the function's name from the fIdent constant processed, however, even then
	 *  some functions (e.g., map) can do simple projections with no function type.  This is permitted.  At this level
	 *  no discrimination of this issue is considered.  We just pick up arg info here and return it for more complete
	 *  analysis up the call chain.
	 *  
	 *  Ordinary functions will have just one argument type array filled;  the "mbr" types will be empty.  The return type 
	 *  string will be filled for all functions.
	 */
	def typeStringFor(node : PmmlExecNode, expandCompoundFieldTypes : Boolean = false) : 
					(Array[(String,Boolean,BaseTypeDef)],Array[(String,Boolean,BaseTypeDef)],ContainerTypeDef,Array[BaseTypeDef], String) = {
	  val typedefInfo : (Array[(String,Boolean,BaseTypeDef)]
			  			,Array[(String,Boolean,BaseTypeDef)]
	  					,ContainerTypeDef
	  					,Array[BaseTypeDef]
			  			,String) = node match {
	    case c : xConstant => {
	    	val typdefs : Array[(String,Boolean,BaseTypeDef)] = constantKeyForSimpleNode(node.asInstanceOf[xConstant], expandCompoundFieldTypes)
	    	(typdefs, null, null, null, null)
	    }
	    case f : xFieldRef => {
	    	val typedefs : Array[(String,Boolean,BaseTypeDef)] = fldRefArgKey(node.asInstanceOf[xFieldRef], expandCompoundFieldTypes)
	    	(typedefs, null, null, null, null)
	    }
	    case a : xApply => {
	    	val (fcnRetType, funcdef, funcArgs, mbrFcn, mbrArgs, container, containerMbrTypes) : 
	    				(String
						, FunctionDef, Array[(String,Boolean,BaseTypeDef)]
						, FunctionDef, Array[(String,Boolean,BaseTypeDef)]
						, ContainerTypeDef
						, Array[BaseTypeDef]) = fcnArgKey(node.asInstanceOf[xApply])
						
	    	(funcArgs, mbrArgs, container, containerMbrTypes, fcnRetType)
	    }
	    case _ => {
	    	logger.error("This kind of function argument is currently not supported") 
	    	(null, null, null, null, null)
	    }
	  }
	  typedefInfo
	}
	
	/** 
	 *  Discover the typestring for the supplied constant that is an argument for 
	 *  a simple function (i.e., not Iterable).  
	 */
	def constantKeyForSimpleNode(constNode : xConstant, expandCompoundFieldTypes : Boolean = false) : Array[(String,Boolean,BaseTypeDef)] = {
	  	val dtype : String = constNode.dataType.toLowerCase()
		val typestring : Array[(String,Boolean,BaseTypeDef)] = dtype match {
			case "ident" => {
				ctx.getFieldType(constNode.Value.toString, expandCompoundFieldTypes)
			}
			case "fident" => {
				logger.error("During function match... usage of 'fIdent' type arguments in simple functions are not currently supported. ")
				logger.error("Their use is for the 'iterable' class functions.")
				Array[(String,Boolean,BaseTypeDef)](("Any",false,null))
			}
			case _ => { /** ordinary constant.. use its dataType to build information */
				val scalaType : String = PmmlTypes.scalaDataType(constNode.dataType)
				val (typestr, typedef) : (String, BaseTypeDef) = ctx.MetadataHelper.getType(constNode.dataType)
				if (typedef == null) {
					logger.error("Unable to find a constant's type for apply function argument ... fcn = '${node.function} ") 
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
		val typeInfo : Array[(String,Boolean,BaseTypeDef)] = ctx.getFieldType(fldNode.field, expandCompoundFieldTypes)
		typeInfo
	}

	/** 
	 *  Discover the typestring for the supplied Apply that is an argument for a simple function (i.e., not Iterable).  
	 *  Of interest here is the return type of the function.  For the simple functions, this is relatively straight forward.
	 *  For the iterable functions, it depends.  SOME of the factors:  
	 *  <ol>
	 *  <li>Filter, partition, and others return the same collection and member type as the iterable function's 
	 *  	receiver (i.e., the first apply arg)</li>
	 *  <li>The map (et al) functions are transformer functions that can return a different member type.
	 *  	For example an array of arrays of ints (see example below) can change the member type where the collection does
	 *   	not change.  However, it is also to actually return one of the collection's (superclass) traits.  In those
	 *    	cases, the function implementation needs to add a cast to the original collection type to keep it simple.
	 *      <p>In same vein, only one member function will be allowed with the current implementation.  Its
	 *     	return type will be used to change the collection's member type(or types)</p>
	 *  </li>
	 *  <li>The groupBy function for example will take an array and split it into parts based upon some discriminator
	 *  	function.  The result is a map! where the key is the group by value determined by the discriminator function
	 *   	and the value is the elements of the array that share that value.
	 *  </ol>
	 *  
	 *  To see the complexity involved resolving inferring the return type, consider this REPL session:
	 *  
	 *  scala> val a : Array[Int] = Array[Int](1,2,3,4,5)
	 *  a: Array[Int] = Array(1, 2, 3, 4, 5)
	 *  
	 *  scala> val b  = a.map(_ * 2)
	 *  b: Array[Int] = Array(2, 4, 6, 8, 10)
	 *  
	 *  scala> val c = b.map(_ + 2)
	 *  c: Array[Int] = Array(4, 6, 8, 10, 12)
	 *  
	 *  scala> val composite = Array[Array[Int]](a,b,c)
	 *  composite: Array[Array[Int]] = Array(Array(1, 2, 3, 4, 5), Array(2, 4, 6, 8, 10), Array(4, 6, 8, 10, 12))
	 *  
	 *  scala> composite.map(arr => arr.sum)  <<< this kind of thing is common for aggregation
	 *  res0: Array[Int] = Array(15, 30, 40)
	 *  
	 *  scala> composite.map(arr => arr(0) + arr(2))  <<< this kind of thing is common for aggregation
	 *  res1: Array[Int] = Array(4, 8, 12)
	 *  
	 *  scala> val z = a zip b
	 *  z: Array[(Int, Int)] = Array((1,2), (2,4), (3,6), (4,8), (5,10))
	 *  
	 *  scala> val zMap = (a zip b).toMap
	 *  zMap: scala.collection.immutable.Map[Int,Int] = Map(5 -> 10, 1 -> 2, 2 -> 4, 3 -> 6, 4 -> 8)
	 *  
	 *  scala> zMap.values
	 *  res2: Iterable[Int] = MapLike(10, 2, 4, 6, 8)
	 *  
	 *  scala> zMap.values.toArray
	 *  res3: Array[Int] = Array(10, 2, 4, 6, 8)
	 *  
	 *  scala> zMap.keys.toArray
	 *  res4: Array[Int] = Array(5, 1, 2, 3, 4)
	 *  
	 *  scala> zMap.keys.toVector
	 *  res5: Vector[Int] = Vector(5, 1, 2, 3, 4)
	 *  
	 *  scala> zMap.keys.toList
	 *  res6: List[Int] = List(5, 1, 2, 3, 4)
	 *  
	 *  scala> zMap.toArray
	 *  res7: Array[(Int, Int)] = Array((5,10), (1,2), (2,4), (3,6), (4,8)) <<< map's k/v transforms to (k,v)
	 *  
	 *  scala> val s : Array[Int] = Array[Int](1,2,3,4,5,6,7,8,9,10)
	 *  s: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	 *  
	 *  scala> val kinds = s.groupBy {
     *  |    case itm if (itm < 3) => "one"
     *  |    case itm if (itm >= 3 && itm <= 6) => "two"
     *  |    case _ => "other"
     *  | }
	 *  kinds: scala.collection.immutable.Map[String,Array[Int]] = 
	 *  		Map(one -> Array(1, 2), two -> Array(3, 4, 5, 6), other -> Array(7, 8, 9, 10))
	 *    
	 *  FIXME: Suffice it to say there are many nuances here.  This function and things revolving around
	 *  the necessary type inference deserve there own source file and class.  
	 *  
	 *  @param fcnNode the pmml function node to be considered 
	 *  @return (the function return type, the function def for the function, the mbr function def if appropriate, the
	 *  	the iterable function's collection type info if appropriate, the iterable function's member type(s) if 
	 *   	appropriate)
	 *  
	 */
	def fcnArgKey(fcnNode : xApply) : 
			(String
			, FunctionDef, Array[(String,Boolean,BaseTypeDef)]
			, FunctionDef, Array[(String,Boolean,BaseTypeDef)]
			, ContainerTypeDef
			, Array[BaseTypeDef]) = {
	  
	  	val fcnNodeSelector : FunctionSelect = new FunctionSelect(ctx, mgr, fcnNode)
		val isIterable = fcnNodeSelector.isIterableFcn
		var typestring : String = null 
		var funcDef : FunctionDef = null
		var mbrFuncDef : FunctionDef = null
		var argTypes : Array[(String,Boolean,BaseTypeDef)] = null 
		var mbrsArgTypes : Array[(String,Boolean,BaseTypeDef)] = null 
		var collectionType : ContainerTypeDef = null
		var collElemTypes : Array[BaseTypeDef] = null
		if (isIterable) { 
			if (fcnNode.function == "ContainerFilter") {
				val wtf : Boolean = true
			}
			val typeInfo : FcnTypeInfo = fcnNodeSelector.selectIterableFcn 
			if (typeInfo != null) {
				funcDef  = typeInfo.fcnDef
				mbrFuncDef  = typeInfo.mbrFcn
				argTypes  = typeInfo.argTypes 
				mbrsArgTypes = typeInfo.mbrArgTypes 
				collectionType = typeInfo.containerTypeDef
				collElemTypes = typeInfo.collectionElementTypes
			}
						
			/** 
			 	FIXME: 
			 	FIXME: 
			 	FIXME: 
			 	FIXME: 
			 	FIXME: Reread the comment above.  Simply returning the Iterables type is likely wrong for many 
			 	collection operations.  A separate class should be implemented dedicated to generating the right
			 	type information for both the collection and its member types.  There are nuances for all of the
			 	different container types and the operations that can be performed on them.
			 	FIXME: 
			 	FIXME: 
			 	FIXME: 
			 	FIXME: This may need to be partially implemented to satisfy pmml like this:
			 	
					<Apply function="Sum">
						<Apply function="ContainerMap">
				       		<FieldRef field="outPatientClaims"/>
				 			<Constant dataType="fIdent">Sum</Constant> 
				 			<Constant dataType="ident">Clm_Pmt_Amt</Constant> 
				 			<Constant dataType="ident">Nch_Prmry_Pyr_Clm_Pd_Amt</Constant> 
							<Constant dataType="ident">Nch_Bene_Blood_Ddctbl_Lblty_Am</Constant> 
							<Constant dataType="ident">Nch_Bene_Ptb_Ddctbl_Amt</Constant> 
							<Constant dataType="ident">Nch_Bene_Ptb_Coinsrnc_Amt</Constant> 
						</Apply>
					</Apply>
			 	
			 	This processes an Array[OutpatientClaim] instances. The result of the ContainerMap 
			 	function should be Array[Double] assuming outPatientClaims is an Array.  In other words
			 	the matching Sum's return type should be used for the member type of the result array.
			 	
			 	In this example, to choose the correct OUTER Sum function, we need to know the return type
			 	of the ContainerMap.  We MAY be able to make this work by using the "sum" builtin 
			 	function instead of "Sum" the function that actually implements it.  This would let the
			 	scala compiler figure it out when compiling generated source... assuming there is an 
			 	implementation of Sum(Array[Double]) which there is.
			 */
			val scalaFcnName : String = PmmlTypes.scalaNameForIterableFcnName(fcnNode.function)
			val isMapFcn : Boolean = (scalaFcnName == "map")
			val isFilterFcn : Boolean = (scalaFcnName == "filter")
			typestring = if (typeInfo != null) {
				if (isMapFcn) {  
					/** 
					 *  
					 *  FIXME: This is clearly inadequate... groupBy et al need to be handled 
					 *  ... not handling those without a member function either 
					 *  
					 */
					val nominalCollectionName = ObjType.asString(collectionType.tType)
					val returnElementTypes : String = determineReturnElementTypes(mbrFuncDef, mbrsArgTypes)
					val returnType : String = s"$nominalCollectionName[$returnElementTypes]"
					returnType
				} else {
					val retTypeString : String = if (isFilterFcn) {
						val collTypeStr : String = collectionType.typeString
						collTypeStr
					} else {
						val funcDefReturnTypeStr : String = funcDef.returnTypeString
						funcDefReturnTypeStr
					}
					retTypeString
				}
			} else {
				"Any"
			}
		} else {
			val typeInfo : FcnTypeInfo = fcnNodeSelector.selectSimpleFcn 
			if (typeInfo != null) {
				funcDef  = typeInfo.fcnDef
				mbrFuncDef  = typeInfo.mbrFcn
				argTypes  = typeInfo.argTypes 
				mbrsArgTypes = typeInfo.mbrArgTypes 
				collectionType = typeInfo.containerTypeDef
				collElemTypes = typeInfo.collectionElementTypes
			}

			typestring = if (typeInfo != null && funcDef != null) {
				funcDef.returnTypeString
			} else {
				"Any"
			}
		}

		(typestring, funcDef, argTypes, mbrFuncDef, mbrsArgTypes, collectionType, collElemTypes)
	}

	/** 
	 *  Answer the return types to use for a collection being created by some function through a map (or similar) transformative
	 *  operation.  There are two cases:
	 *  
	 *  <ul>
	 *  <li>When there is a member function present (there is an fIdent constant type denoting the fcn name), use the function definition's
	 *  	return type.  This determination is made in the caller.  Here we simply see if there is a metadata typedef for the function.
	 *  </li>
	 *  <li>When there is NO member function present, the return type is based upon the free arguments in the mbrsArgTypes.  Typically there
	 *  	is one argument, however if there are multiple arguments, a tuple type is formed from the arg metadata.
	 *  </li>
	 *  </ul>
	 *  
	 *  @param mbrFuncDef - the metadata for the iterable's member function.  If null the mbrArgTypes will be used to form the return element types
	 *  @param mbrsArgTypes - and array of the type info triples (typeStr, isContainerWithFields, typeInfo metadata) for the member function 
	 *  	arguments (or the args to be used in a projection of a structure or mapped base container)
	 *  @return memberType string to be used as part of the return type for some iterable collection being iterated.
	 */
	def determineReturnElementTypes(mbrFuncDef : FunctionDef, mbrsArgTypes : Array[(String,Boolean,BaseTypeDef)] ) : String = {
	  	
  		val returnElementTypes  = if (mbrFuncDef != null) {
  			mbrFuncDef.returnTypeString
  		} else {
  			val argCnt : Int = mbrsArgTypes.size
  			val buffer : StringBuilder = new StringBuilder
  			var cnt : Int = 0
  			val nonNullArgCnt : Int = mbrsArgTypes.filter(_ != null).size
  			if (nonNullArgCnt > 1) {
  				buffer.append("(")
  			}
  			mbrsArgTypes.foreach(mbr => {
  				if (mbr != null) {
  					cnt += 1
					val (typestr, isContainerWithFields, typedef) : (String,Boolean,BaseTypeDef) = mbr 
					if (typestr != null) {
						buffer.append(typestr)
					}
					if (cnt < nonNullArgCnt) {
						buffer.append(",")
					}
  				}
  			})
  			if (argCnt > 1) {
  				buffer.append(")")
  			} 
  			buffer.toString
  		}
  		returnElementTypes
	}
	
	
	/** 
	 *  If the straight forward search key produced does not produce a FunctionDef, this more
	 *  elaborate mechanism is used to try to relax the argument types.
	 *  NOTE: This is a work in progress. First implementation will look for scalar arguments
	 *  of different widths and relax the width iff there are functions with a wider width
	 *  than was proposed in the first key produced.
	 */
	def relaxIterableKeys(doElemFcn : Boolean
						, iterableFcnName : String
						, iterableFcnArgs: Array[(String,Boolean,BaseTypeDef)]
						, elemFcnName : String
						, elemFcnArgs : Array[(String,Boolean,BaseTypeDef)]
						, containerType: ContainerTypeDef
						, collectionsElementTypes : Array[BaseTypeDef]
						, returnTypes : Array[String]) : Array[String] = {
	  	/** 
	  	 *  For now there is no special treatment different than that used for simple functions...
	  	 *  it is just that both the iterable function and the member function (assuming there is one)
	  	 */
	  	val hasFIdent : Boolean = checkForMemberFcn
	  	val reasonableArgMinimum : Int = if (hasFIdent) 2 else 1 

	  	if (returnTypes.size <= reasonableArgMinimum) {
	  		logger.error("Insufficient return types to split in between iterable portion and member function portion")
	  	}
	  	val iterableRetTypes : Array[String] = returnTypes.take(2)
	  	val elemRetTypes : Array[String] = returnTypes.tail.tail
		val signatures : Array[String] = if (doElemFcn) {
			if (elemFcnName != null && elemFcnName != "") {
				relaxSimpleKey(elemFcnName, elemFcnArgs, elemRetTypes)
			} else {
				Array[String]()
			}
		} else {
			relaxSimpleKey(iterableFcnName, iterableFcnArgs, iterableRetTypes)			
		}
	  			
		signatures
	}
	
	/** This function creates type information for the arguments to the Iterable's member function 
	 *  An array is returned of the type information since the simple function treatment does that.
	 *  
	 *  Note: For the time being we will not accept xApply types within an iterable function.
	 *  This may be relaxed in the future.
	 *  
	 *  collectionsElementTypes contain one or more element types. Lists, arrays, sets, and the like
	 *  have one typedef there, maps have two, and tuples could have an arbitrary number of types.
	 *  
	 */
	def typeDefsForIterablesFunction(node : PmmlExecNode
									, expandCompoundFieldTypes : Boolean
									, containerType : ContainerTypeDef
									, collectionsElementTypes : Array[BaseTypeDef]) : Array[(String,Boolean,BaseTypeDef)] = 
	{
		val typedefs : Array[(String,Boolean,BaseTypeDef)] = node match {
		    case c : xConstant => {
		    	val constNode : xConstant = node.asInstanceOf[xConstant]
		    	constantArgForIterableFcn(constNode, expandCompoundFieldTypes, containerType, collectionsElementTypes)
		    }
		    case f : xFieldRef => fldRefArgKey(node.asInstanceOf[xFieldRef], expandCompoundFieldTypes)
		    case a : xApply => {
		    	logger.warn("A sub-function within an iterable function is not currently supported")
		    	Array[(String,Boolean,BaseTypeDef)](("NoSubFcnsSupportedInIterableFcnSoFar",false,null))
		    }
		    case _ => {
		    	logger.error("This kind of function argument is currently not supported... only xConstant and xFieldRef nodes can be used for iterable functions") 
		    	Array[(String,Boolean,BaseTypeDef)](("None",false,null))
		    }
		}
		typedefs
	}
	

	/** 
	 *  Discover the typestring for the supplied constant that is an argument for 
	 *  a simple function (i.e., not Iterable).  
	 */
	def constantArgForIterableFcn(constNode : xConstant
								, expandCompoundFieldTypes : Boolean
								, containerType : ContainerTypeDef
								, collectionsElementTypes : Array[BaseTypeDef]) : Array[(String,Boolean,BaseTypeDef)] = {
	  	val dtype : String = constNode.dataType.toLowerCase()
		val typedefs : Array[(String,Boolean,BaseTypeDef)] = dtype match {
	  		/** FIXME: .. an ident could very well be a container */
			case "ident" => { 
			  /** FIXME: The identifier value could be referencing a subcontainer's content... i.e., it has a dot or dots in
			   *  its value.  We need to support this.  For now flag it in the log 
			   */
			  val hasDots : Boolean = constNode.Value.toString.contains('.')
			  if (hasDots) {
				  logger.warn("subcontainer references currently not supported... stay tuned")
			  } 
			  /** 
			   *  The containerType is some sort of collection...The "ident" value actually references a structure or 
			   *  mapped member within the container.  In other words, (one of) the collectionsElementTypes contains the "ident" 
			   *  field.  
			   *  
			   *  For arrays, lists, queues, stacks, sets, and other single member types, it is straight forward.  For maps with
			   *  two members, we are going to assume it is in the Value type.  NOTE: That could in fact be wrong.  We will revisit
			   *  this if necessary.
			   *  
			   *  If it is a tuple with arbitrary count of element types, I suppose it could be in any container that is maintained in
			   *  the tuple.  Perhaps we search for it.  At the moment, we won't support tuple dereferencing...
			   *  
			   */
			  val leafElementType : BaseTypeDef = if (collectionsElementTypes.size > 0) collectionsElementTypes.last else null
			  if (leafElementType != null){
				  /** search the function's iterable container for this (attr) name */
				  val attrContainer  : ContainerTypeDef = if (ctx.MetadataHelper.isContainerWithFieldOrKeyNames(leafElementType)) {
					  leafElementType match {
					    case s : StructTypeDef => leafElementType.asInstanceOf[StructTypeDef]
					    case s : MappedMsgTypeDef => leafElementType.asInstanceOf[MappedMsgTypeDef]
					    case _ => null
					  }
				  } else {
					  null
				  }
			      if (attrContainer != null) {
			    	  /** see if the "ident" value is one of the fields in the fields of the container */
			    	  val attr : BaseAttributeDef = if (attrContainer.isInstanceOf[StructTypeDef]) {
			    		  attrContainer.asInstanceOf[StructTypeDef].attributeFor(constNode.Value.toString)
			    	  } else {
			    		  attrContainer.asInstanceOf[MappedMsgTypeDef].attributeFor(constNode.Value.toString)
			    	  }
			    	  val (attrType, attrStr, isContainerWithFields) : (BaseTypeDef, String, Boolean) = if (attr != null) {
				    	  val attrType : BaseTypeDef = attr.typeDef
				    	  val attrStr = attrType.typeString
				    	  val isContainerWithFields = ctx.MetadataHelper.isContainerWithFieldOrKeyNames(attrType)
				    	  (attrType, attrStr, isContainerWithFields)
			    	  } else {
			    		  (null,"Any",false)
			    	  }
			    	  Array[(String,Boolean,BaseTypeDef)]((attrStr,isContainerWithFields,attrType))
			      } else {
			    	  logger.warn(s"Ident const ${constNode.Value.toString} does not reference any field in iterable collection, ${containerType.typeString}")
			    	  Array[(String,Boolean,BaseTypeDef)](("Unknown field",false,null))
			      }
			  } else {
				  /** 
				   *  This is not going well.. not a member of the iterable collection ... 
				   *  try to use the constant name for standard field name search...before crash and burn
				   */
				  ctx.getFieldType(constNode.Value.toString, expandCompoundFieldTypes)
			  }
			} 
			case "fident" => {
				/** For fIdent, there is no arg info to collect.  Functions higher up the chain will use the
				 *  value for this constant as the function name.  Return the name in the position of the typestring 
				 */
				val fcnName : String = constNode.Value.toString
				logger.trace(s"constantArgForIterableFcn - fIdent value : $fcnName")
				Array[(String,Boolean,BaseTypeDef)]((fcnName,false,null))
			}
			case _ => { /** ordinary constant.. use its dataType to build information */
				val scalaType : String = PmmlTypes.scalaDataType(constNode.dataType)
				val (typestr, typedef) : (String, BaseTypeDef) = ctx.MetadataHelper.getType(constNode.dataType)
				if (typedef == null) {
					logger.error("Unable to find a constant's type for apply function argument ... fcn = '${node.function} ") 
				}
				val isContainerWithFieldNames : Boolean = ctx.MetadataHelper.isContainerWithFieldOrKeyNames(typedef)
				Array[(String,Boolean,BaseTypeDef)]((typestr, isContainerWithFieldNames, typedef))
			}   
		}
	  	typedefs
	}

}
