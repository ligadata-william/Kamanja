package com.ligadata.udf.extract

import scala.collection.mutable._
import util.control.Breaks._
import scala.util.matching.Regex
import org.apache.log4j.Logger
import com.ligadata.olep.metadata._


/** 
 	Class MethodCmd
 	
 	Prepare a MdMgr.MakeFunc command from the method information gleaned from an full object path sent to the MethodExtract.
 	As a side effect, update the typeMap with any types that are found in the object's methods' type signature.
 	
    Turn method information like this:
    
	    method MakeStrings ... 
	  		name = MakeStrings, 
	    	fullName = com.ligadata.pmml.udfs.Udfs.MakeStrings, 
	     	returnType = scala.Array[String], 
	      	typeSig = (arr: scala.Array[(String, String)], separator: String)scala.Array[String] 
	      	
		method Plus ... 
			name = Plus, 
			fullName = com.ligadata.pmml.udfs.Udfs.Plus, 
			returnType = scala.Float, 
			typeSig = (expr1: scala.Float, expr2: scala.Int)scala.Float
    
    into this:
       
		MdMgr.MakeFunc(mgr
				, MdMgr.sysNS, "Get"
				, (MdMgr.sysNS,"Any")
				, List(("gCtx", MdMgr.sysNS, "EnvContext")
				    ,("containerId", MdMgr.sysNS, "String")
				    ,("key", MdMgr.sysNS, "Int"))
				, false)   
 
		MdMgr.MakeFunc(mgr, "<namespace>", MdMgr.sysNS, "Get" , ("System","Any") , List(("gCtx", "System", "EnvContext") ,("containerId", "System", "String") ,("key", "System", "Int")), false)   
 		where namespace is the value passed to the MethodExtract application from user.
 		
 	In addition, there may be numerous types that are used in the methods that are not defined in the metadata.  To this end,
 	a Map[String, String] where the key is the namespace qualified name of the type and the value is the appropriate "Make" command 
 	for that type.
 	
 	For the collections, the name of the type generated will be the container type + "of" + the name of the element(s).
 	For example, a Set of Int is named SetOfInt, a List of Float is ListOfFloat, an ArrayBuffer of String is an ArrayBufferOfString,
 	a Map of String,Int pairs is a MapOfStringInt.
 	
 	These "Make" types strings will be emitted just like the "MakeFunc" strings. 
 	
 	@param logger the log4j logger in which to deposit diagnostics
 	@param namespace the command line supplied namespace name to be used to catalog the methods from the command line supplied object.
 	@param typeMap this map will be updated with the types that are used by the methods being processed in the object.
 	@param name this is the simple name of the function currently being processed.
 	@param fullName this is the full package qualified name of the function being processed.
 	@param returnType this is the return type of the function being processed
 	@param typeSig is the argument type list and the the return type suffixed to it of the current method
 	
 */

class MethodCmd(val logger : Logger
			    , val namespace : String
			    , var typeMap : Map[String, String]
			    , var typeArray : ArrayBuffer[String]
			    , val name : String
			    , val fullName : String
			    , val returnType : String
			    , val typeSig : String) {
  
	//logger.trace(s"namespace=$namespace, name=$name, returnType=$returnType, typeSig=$typeSig")
	override def toString : String = {

	  	val buffer : StringBuilder = new StringBuilder()
	  	val returnTypeVal : String = ReturnType
	  	val argNm_NmSpc_TypNm : Array[String] = TypeSig

	  	buffer.append(s"MdMgr.MakeFunc(mgr, ${'"'}$namespace${'"'}, ")
	  	val rStr = returnTypeVal.toString
	  	val retElements : Array[String] = rStr.split('.')
	  	val retTypeStr = if (retElements.size == 2) {
	  		val rnmspc : String = retElements(0)
	  		val rnm : String = retElements(1)
	  		s"${'"'}$rnmspc${'"'}, ${'"'}$rnm${'"'}"
	  	} else {
	  		logger.error("The type generation has failed in some way for return type $rStr of function $fullName with args $typeSig" )
	  		"bogus return type"
	  	}

	  	buffer.append(s"${'"'}$name${'"'}, ($retTypeStr), List(")
	  	
	  	val argbuffer : StringBuilder = new StringBuilder
	  	argNm_NmSpc_TypNm.addString(argbuffer, ",")
	  	val argsStr : String = argbuffer.toString
	  	buffer.append(s"$argsStr")
	  	
	  	buffer.append(s"), null)\n")
	  	buffer.toString
	} 
	
	/** 
	 *  Answer the return type string with a namespace injected as necessary.
	 */
	def ReturnType : String = {
		collectType(returnType.split('.'), returnType)
	}

	/**
	 * 	Create an array of type string arguments, including variable names from the supplied typeSig on the ctor.
	 *  These strings look like this:
	 *  
	 *  	typeSig = (expr1: scala.Float, expr2: scala.Int)scala.Float
	 *   	typeSig = (arr: scala.Array[(String, String)], separator: String)scala.Array[String]
	 *    
	 *  Like the return type, each arg type is sought in the typeMap supplied to the ctor.  If it is present,
	 *  the type is used from the map.  If not present, the namespace qualified typename is used as the key
	 *  and a make<Type> command string is added to the map for it... any {MakeArray, MakeArrayBuffer, MakeSet, MakeMap, MakeList}
	 *  
	 *  @return a type string for each argument.
	 */
	def TypeSig : Array[String] = {
	  
		val typeSigArgs : String = encloseElementArgs(typeSig, '(', ')')
		val nm_nmspc_typnm : Array[String] = if (typeSigArgs == "") {
			Array[String]()
		} else {	
			val reNames = "[a-zA-Z0-9_]+:".r
			var argNames : Array[String] = null
			val reTypes = "[a-zA-Z0-9_]+:|, [a-zA-Z0-9_]+: "
			var argTypes : Array[String] = null
		  
			try {
				argNames = ((reNames findAllIn typeSigArgs).mkString(",")).split(',').map(nm => nm.stripSuffix(":"))
				argTypes = typeSigArgs.split(reTypes).filter(_.length > 0).map(typ => typ.trim)
			} catch {
				case e : Exception => {	}
			}
			
			if (argNames.size == 0 || argTypes.size == 0) {
				Array[String]()
			} else {
				/** a few transformations... */
				
	
				/** Collect the types of each argument */
				val argsWithNmSpcFixed : Array[String] = argTypes.map( argtype => {
					val argNodes : Array[String] = argtype.split('.')
					collectType(argNodes, argtype)
				})
				
				/** transform the names and the argtype namespace and typename to a triple */
				val nameAndTypePair = argNames.zip(argsWithNmSpcFixed)
				val nameNmSpcTypNameTriples : Array[(String,String,String)] = nameAndTypePair.map( pair => {
				  val (name, typepair) = pair
				  val typePairSplit = typepair.split('.')
				  val quotedTypePair  = typePairSplit.map(itm => s"${'"'}$itm${'"'}")
				  val quotedName = s"${'"'}$name${'"'}"
				  (quotedName,quotedTypePair(0),quotedTypePair(1))
				})
				/** transform the triple into a single string */
				val nmnmspctyptrip : Array[String] =  nameNmSpcTypNameTriples.map ( tup => {
					val (nm,nmspc,typnm) = tup
					s"($nm, $nmspc, $typnm)"
				})
				
				nmnmspctyptrip
			}
		}
		nm_nmspc_typnm
	}

	/** 
	 *  The current type being processed (either from the return type or one of the function arguments) has more than
	 *  two nodes delimited by dots (e.g., scala.collection.mutable.ArrayBuffer[Float]).  Determine the generated name
	 *  for this type, see if it is present in the typeMap passed to the primary constructor.  If not present, create a 
	 *  the appropriate Make<Type> string for it ... any {MakeArray, MakeArrayBuffer, MakeSet, MakeMap, MakeList}
	 *  
	 *  @param typeParts the nodes of the type string split on the dots
	 *  @return type string for this type fully qualified with '.' as delimiter
	 */
	def collectType(typeParts : Array[String], typeString : String) : String = {
	  	val typeCollected = if (typeParts.size == 0) "" else {
			val unQualifiedTypeName = typePartsLast(typeParts, typeString) 
			val typeName : String = unQualifiedTypeName match {
				case "List" => ListType(unQualifiedTypeName, typeParts)
				case "Array" => ArrayType(unQualifiedTypeName, typeParts)
				case "Array[T]" => ArrayType(unQualifiedTypeName, typeParts)
				case "ArrayBuffer" => ArrayBufferType(unQualifiedTypeName, typeParts)
				case "ArrayBuffer[T]" => ArrayBufferType(unQualifiedTypeName, typeParts)
				case "Map" => MapType(unQualifiedTypeName, typeParts)
				case "Map[A,B]" => MapType(unQualifiedTypeName, typeParts)
				case "Set" => SetType(unQualifiedTypeName, typeParts)
				case "TreeSet" => TreeSetType(unQualifiedTypeName, typeParts)			
				case "TreeSet[T]" => TreeSetType(unQualifiedTypeName, typeParts)			
				case "Set[T]" => SetType(unQualifiedTypeName, typeParts)
				case _ => {
					if (unQualifiedTypeName.startsWith("List")) {
						ListType(unQualifiedTypeName,typeParts)
					} else {
						if (unQualifiedTypeName.startsWith("ArrayBuffer")) {
							ArrayBufferType(unQualifiedTypeName,typeParts)
						} else {
							if (unQualifiedTypeName.startsWith("Array")) {
								ArrayType(unQualifiedTypeName,typeParts)
							} else {
								if (unQualifiedTypeName.startsWith("Map")) {
									MapType(unQualifiedTypeName,typeParts)				
								} else {
									if (unQualifiedTypeName.startsWith("TreeSet")) {
										TreeSetType(unQualifiedTypeName,typeParts)
									} else {
										if (unQualifiedTypeName.startsWith("SortedSet")) {
											SortedSetType(unQualifiedTypeName,typeParts)
										} else {
											if (unQualifiedTypeName.startsWith("Set")) {
												SetType(unQualifiedTypeName,typeParts)
											} else {
												if (unQualifiedTypeName.startsWith("Queue")) {
													QueueType(unQualifiedTypeName,typeParts)
												} else {
													if (unQualifiedTypeName.startsWith("HashMap")) {
														HashMapType(unQualifiedTypeName,typeParts)
													} else {
														SimpleType(unQualifiedTypeName, typeParts)
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			typeName
	  	}
	  	typeCollected
	}
	
	/** 
	 *  A List type with the item qualifier in brackets is presented. 
	 *  e.g., scala.collection.mutable.List[Float] => System.ListOfFloat
	 */
	def ListType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

  		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.ListOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeList(mgr, $nmspc, $nm, $itmnmspc, $itmnm)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  A Queue type with the item qualifier in brackets is presented. 
	 *  e.g., scala.collection.mutable.Queue[Float] => System.QueueOfFloat
	 */
	def QueueType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

  		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.QueueOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeQueue(mgr, $nmspc, $nm, $itmnmspc, $itmnm)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  An Array type with the element qualifier in brackets is presented. 
	 *  e.g., scala.collection.mutable.Array[Float] => System.ArrayOfFloat
	 */
	def ArrayType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.ArrayOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeArray(mgr, $nmspc, $nm, $itmnmspc, $itmnm, 1)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  An ArrayBuffer type with the element qualifier in brackets is presented. 
	 *  e.g., scala.collection.mutable.ArrayBuffer[Float] => System.ArrayBufferOfFloat
	 */
	def ArrayBufferType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.ArrayBufferOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeArrayBuffer(mgr, $nmspc, $nm, $itmnmspc, $itmnm, 1)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  A Map type with the key/value in brackets is presented. 
	 *  e.g., scala.collection.mutable.Map[String, Float] => System.MapOfStringFloat
	 */
	def MapType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.MapOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (keynmspc,keynm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val (valnmspc,valnm) : (String, String) = itmNmSpcdAndQuoted(1)	
			val cmd : String = s"MdMgr.MakeMap(mgr, $nmspc, $nm, ($keynmspc, $keynm), ($valnmspc, $valnm))"
			typeMap(key) = cmd
			typeArray += cmd
	   	}
	  	key
	}
	
	/** 
	 *  A HashMapType type with the key/value in brackets is presented. 
	 *  e.g., scala.collection.mutable.Map[String, Float] => System.MapOfStringFloat
	 */
	def HashMapType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.HashMapOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (keynmspc,keynm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val (valnmspc,valnm) : (String, String) = itmNmSpcdAndQuoted(1)	
			val cmd : String = s"MdMgr.MakeHashMap(mgr, $nmspc, $nm, ($keynmspc, $keynm), ($valnmspc, $valnm))"
			typeMap(key) = cmd
			typeArray += cmd
	   	}
	  	key
	}
	
	/** 
	 *  A Set type with the key in brackets is presented. 
	 *  e.g., scala.collection.mutable.Set[String]
	 */
	def SetType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.SetOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeSet(mgr, $nmspc, $nm, $itmnmspc, $itmnm)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  A TreeSet type with the key in brackets is presented. 
	 *  e.g., scala.collection.mutable.TreeSet[String]
	 */
	def TreeSetType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.TreeSetOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeTreeSet(mgr, $nmspc, $nm, $itmnmspc, $itmnm)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  A SortedSet type with the key in brackets is presented. 
	 *  e.g., scala.collection.mutable.SortedSet[String]
	 */
	def SortedSetType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
  		val isImmutable : Boolean = (typeParts.filter(part => part == "immutable").size > 0)

 		val itemKey = formKeyFromElementSpecifiers(unQualifiedTypeName)
		val key = s"System.SortedSetOf$itemKey"
		if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val itmNmSpcdAndQuoted : Array[(String,String)] = formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName)
			val (itmnmspc,itmnm) : (String, String) = itmNmSpcdAndQuoted(0)	
			val cmd : String = s"MdMgr.MakeSortedSet(mgr, $nmspc, $nm, $itmnmspc, $itmnm)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 
	 *  A scalar or other simple type 
	 */
	def SimpleType(unQualifiedTypeName : String, typeParts : Array[String]) : String = {
	  	val key : String = s"System.$unQualifiedTypeName"
	  	if (! typeMap.contains(key)) {
			val nmspcAndNm : Array[String] = splitAndQuote(key)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val typ : ObjType.Type = ObjType.fromString(key.split('.').last)
			val cmd : String = s"MdMgr.MakeScalar(mgr, $nmspc, $nm, $typ)"
			typeMap(key) = cmd
			typeArray += cmd
	  	}
	  	key
	}
	
	/** 		
		MdMgr.MakeTupleType(for argKey = TupleOfStringString...tbd)
 		def MakeTupleType(mgr : MdMgr, nameSpace: String, name:String, tuples : Array[(String,String)])
	 */
	
	def TupleType(tupleTypeStr : String) : String = {
		val buffer : StringBuilder = new StringBuilder()
		buffer.append("System.TupleOf")
		val args : String = encloseElementArgs(tupleTypeStr, '(', ')')  
	  	val types : Array[String] = args.split(',')
	  	if (args == "T") {
	  		buffer.append("Any")
	  	} else {
	  		val typesWithoutDots : Array[String] = types.map( typ => typ.split('.').last.trim)
	  		typesWithoutDots.addString(buffer, "")
	  	}		
	  	val argKey : String = buffer.toString
		if (! typeMap.contains(argKey)) {
			val nmspcAndNm : Array[String] = splitAndQuote(argKey)
			val (nmspc,nm) : (String, String) = (nmspcAndNm(0), nmspcAndNm(1))
			val typesWithoutDots : Array[String] = types.map( typ => typ.split('.').last.trim)
			val nmspcAndNmsSansDots : Array[(String,String)] = typesWithoutDots.map( typ => (s"${'"'}System${'"'}", s"${'"'}$typ${'"'}") )
			val tupBuf : StringBuilder = new StringBuilder()
			tupBuf.append(s"Array(")
			nmspcAndNmsSansDots.addString(tupBuf, ", ")
			tupBuf.append(s")")
			val tuples : String = tupBuf.toString
			val cmd : String = s"MdMgr.MakeTupleType(mgr, $nmspc, $nm, $tuples)"
			typeMap(argKey) = cmd
			typeArray += cmd
	  	}
	  	
	  	argKey
	}


	def formKeyFromElementSpecifiers(unQualifiedTypeName : String) : String = {
		val buffer : StringBuilder = new StringBuilder()
		val args : String = encloseElementArgs(unQualifiedTypeName, '[', ']')
	  	val types : Array[String] = args.split(',')
	  	val typesRecursed : Array[String] = types.map( typ => {
	  		if (typ.contains("[")) {
	  			collectType(typ.split('.'), typ)
	  		} else {
	  			typ
	  		}
	  	})
	  	if (args == "T") {
	  		buffer.append("Any")
	  	} else {
	  		val typesWithoutDots : Array[String] = typesRecursed.map( typ => typ.split('.').last)
	  		typesWithoutDots.addString(buffer, "")
	  	}		
	  	val argKey : String = buffer.toString
	  	argKey
	}
	
	/** Each argument type of the element arguments, pair with the System namespace and quote them each */
	def formQuotedNmSpcQualifedElementSpecifiers(unQualifiedTypeName : String) : Array[(String,String)] = {
		val args : String = encloseElementArgs(unQualifiedTypeName, '[', ']')
	  	val types : Array[String] = args.split(',')
	  	val typesRecursed : Array[String] = types.map( typ => {
	  		if (typ.contains("[")) {
	  			collectType(typ.split('.'), typ)
	  		} else {
	  			typ
	  		}
	  	})
	  	val nmspcdAndQuoted = if (args == "T") {
	  		Array((s"${'"'}System${'"'}",s"${'"'}Any${'"'}"))
	  	} else {
	  		val typesWithoutDots : Array[String] = typesRecursed.map( typ => typ.split('.').last)
	  		typesWithoutDots.map( typ => (s"${'"'}System${'"'}", s"${'"'}$typ${'"'}"))
	  	}		
	  	nmspcdAndQuoted
	}
	
	/**
	 * Used by the various type makers above, split the qualified name into a namespace
	 * and name, returning the values as quoted strings.
	 */
	def splitAndQuote(qualifiedTypeName : String) : Array[String] = {
		val splitAndQuoted : Array[String] = qualifiedTypeName.split('.').map( itm => {
			s"${'"'}$itm${'"'}"
		})
		/** there should only be two elements post split */
		splitAndQuoted
	}
	
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
	 *  get the last portion of the name, including any item notation 
	 *  
	 *  Handle things like this:
	 *  	scala.collection.mutable.HashMap[scala.Int,scala.Int]
	 *  as well as:
	 *  	Int, scala.Int, etc
	 *  
	 */
	def typePartsLast(typeParts : Array[String], typeString : String) : String = {
	  
		/** for the ordinary case, the . split done by the caller is adequate.  For a case where dots
		 *  appear in the collection element spec, the dots break badly.  Therefore we look relook 
		 *  at the originating string passed as the 2nd arg to determine the appropriate last type part. */
		var idx : Int = 0
		breakable {
			typeParts.foreach( itm => {
				if (itm.contains("["))
					break
				idx += 1
			})
		}
		val lastPart : String = if (idx < typeParts.size) {
			val elementArgPart : String = encloseElementArgs(typeString, '[', ']')
			val elementArgPartsChecked : String = if (elementArgPart(0) == '(') /** check for tuple ... when true transform type TupleOf<whatever is in the ()> */
				TupleType(elementArgPart)
			else
				elementArgPart
			val collectionTypeWithOpenBracket : String = typeParts(idx)
			/** scrape off the bracket and any part of the element type that may be after it */
			val collectionType : String = collectionTypeWithOpenBracket.split('[').head
			/** reassemble the last parts so as to construct the full type name ... the dots in the item spec for a collection case */
			val buf : StringBuilder = new StringBuilder
			buf.append(collectionType)
			buf.append("[")
			buf.append(elementArgPartsChecked)
			buf.append("]")
			buf.toString
		} else {
			typeParts.last
		}
		lastPart
	}
}

