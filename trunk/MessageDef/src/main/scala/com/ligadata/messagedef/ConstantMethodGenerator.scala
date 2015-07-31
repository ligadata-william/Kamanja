package com.ligadata.messagedef

class ConstantMethodGenerator {
  //populate method in msg-TransactionMsg class
  def populate = {
    """
  def populate(inputdata:InputData) = {
	  if (inputdata.isInstanceOf[DelimitedData])	
		populateCSV(inputdata.asInstanceOf[DelimitedData])
	  else if (inputdata.isInstanceOf[JsonData])
			populateJson(inputdata.asInstanceOf[JsonData])
	  else if (inputdata.isInstanceOf[XmlData])
			populateXml(inputdata.asInstanceOf[XmlData])
	  else throw new Exception("Invalid input data")
    
    }
		"""
  }

  /**
   * //populateCSV fucntion in meg class
   * def populatecsv = {
   * """
   * private def populateCSV(inputdata:DelimitedData): Unit = {
   * inputdata.curPos = assignCsv(inputdata.tokens, inputdata.curPos)
   * }
   * """
   * }
   */
  def populateMappedCSV(assignCsvdata: String, count: Int): String = {
    """
  private def populateCSV(inputdata:DelimitedData): Unit = {
	val list = inputdata.tokens
    val arrvaldelim = "~"
	try{
""" + "\t\tif(list.size < " + (count - 1) + ") throw new Exception(\"Incorrect input data size\")" + """
  """ + assignCsvdata +
      """
 	}catch{
		case e:Exception =>{
			e.printStackTrace()
  			throw e
		}
	}
  }
	  """
  }

  ////populateCSV fucntion in msg class
  def populatecsv(assignCsvdata: String, count: Int): String = {
    """
	  private def populateCSV(inputdata:DelimitedData): Unit = {
		val list = inputdata.tokens
	    val arrvaldelim = "~"
		try{
	""" + "\t\tif(list.size < " + (count - 1) + ") throw new Exception(\"Incorrect input data size\")" + """
	  """ + assignCsvdata +
      """
	 	}catch{
			case e:Exception =>{
				e.printStackTrace()
	  			throw e
			}
		}
	  }
		  """
  }

  def populateJson = {
    """
  private def populateJson(json:JsonData) : Unit = {
	try{
         if (json == null || json.cur_json == null || json.cur_json == None) throw new Exception("Invalid json data")
     	 assignJsonData(json)
	}catch{
	    case e:Exception =>{
   	    	e.printStackTrace()
   	  		throw e	    	
	  	}
	  }
	}
	  """
  }

  def collectionsStr = {
    """
    def CollectionAsArrString(v: Any): Array[String] = {
	  if (v.isInstanceOf[Set[_]]) {
	  	return v.asInstanceOf[Set[String]].toArray
	  }
	  if (v.isInstanceOf[List[_]]) {
	  	return v.asInstanceOf[List[String]].toArray
	  }
	  if (v.isInstanceOf[Array[_]]) {
     	return v.asInstanceOf[Array[String]].toArray
	  }
	  throw new Exception("Unhandled Collection")
	 }
    """

  }

  def caseSensitveMapStr(msg: Message): String = {
    var caseSensMapStr: String = ""

    if (msg.isCase) {
      caseSensMapStr = """
        val map = json.cur_json.get.asInstanceOf[Map[String, Any]]
    		if (map == null) {
    		   throw new Exception("Invalid json data")
    		}
        """

    } else {

      caseSensMapStr = """
       	val mapOriginal = json.cur_json.get.asInstanceOf[Map[String, Any]]
       	if (mapOriginal == null)
        	throw new Exception("Invalid json data")
       
       	val map : scala.collection.mutable.Map[String, Any] =  scala.collection.mutable.Map[String, Any]()
       	mapOriginal.foreach(kv => {map(kv._1.toLowerCase()) = kv._2 } )      
    
        """

    }

    caseSensMapStr

  }

  def assignJsonData(assignJsonData: String, msg: Message) = {

    collectionsStr +
      """
    private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var list : List[Map[String, Any]] = null 
	try{ 
	  """ + caseSensitveMapStr(msg) + """
	  """ + assignJsonData +
      """
	  }catch{
  			case e:Exception =>{
   				e.printStackTrace()
   			throw e	    	
	  	}
	}
  }
	"""
  }
  def assignMappedJsonData(assignJsonData: String, msg: Message) = {

    collectionsStr +
      """
   	def ValueToString(v: Any): String = {
		if (v.isInstanceOf[Set[_]]) {
      		return v.asInstanceOf[Set[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[List[_]]) {
      		return v.asInstanceOf[List[_]].mkString(",")
	  	}
	  	if (v.isInstanceOf[Array[_]]) {
      		return v.asInstanceOf[Array[_]].mkString(",")
	  	}
	  	v.toString
	}
    
   private def assignJsonData(json: JsonData) : Unit =  {
	type tList = List[String]
	type tMap = Map[String, Any]
	var list : List[Map[String, Any]] = null 
    var keySet: Set[Any] = Set();
	try{
	  """ + caseSensitveMapStr(msg) + """
	  	var msgsAndCntrs : scala.collection.mutable.Map[String, Any] = scala.collection.mutable.Map[String, Any]()
    
	  	// Traverse through whole map and make KEYS are lowercase and populate
	  	map.foreach(kv => {
        	val key = kv._1.toLowerCase
        	val typConvidx = keys.getOrElse(key, -1)
        	if (typConvidx > 0) {
          	// Cast to correct type
          	val v1 = typs(typConvidx).Input(kv._2.toString)
	  		// println("==========v1"+v1)
          	fields.put(key, (typConvidx, v1))
        } else { // Is this key is a message or container???
          if (messagesAndContainers(key))
            msgsAndCntrs.put(key, kv._2)
          else if (collectionTypes(key)) {
            // BUGBUG:: what to dfo?
          } else
            fields.put(key, (0, ValueToString(kv._2)))
        }
      })
    """ + assignJsonData +
      """
     // fields.foreach(field => println("Key : "+ field._1 + "Idx " + field._2._1 +"Value" + field._2._2 ))
   
	  } catch {
      	case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }
  """
  }

  def addInputJsonLeftOverKeys = {
    """
        var dataKeySet: Set[Any] = Set();
        dataKeySet = dataKeySet ++ map.keySet
        dataKeySet = dataKeySet.diff(keySet)
        var lftoverkeys: Array[Any] = dataKeySet.toArray
        for (i <- 0 until lftoverkeys.length) {
        	val v = map.getOrElse(lftoverkeys(i).asInstanceOf[String], null)
         	if (v != null)
          	fields.put(lftoverkeys(i).asInstanceOf[String], v)
      	}
   """
  }
  def populateXml = {
    """
  private def populateXml(xmlData:XmlData) : Unit = {	  
	try{
    	val xml = XML.loadString(xmlData.dataInput)
    	assignXml(xml)
	} catch{
		case e:Exception =>{
   			e.printStackTrace()
   	  		throw e	    	
    	}
	}
  }
	  """
  }

  def assignXmlData(xmlData: String) = {
    """
  private def populateXml(xmlData:XmlData) : Unit = {
	try{
	  val xml = XML.loadString(xmlData.dataInput)
	  if(xml == null) throw new Exception("Invalid xml data")
""" + xmlData +
      """
	}catch{
	  case e:Exception =>{
	    e.printStackTrace()
		throw e	    	
	  }
   	}
  }
"""
  }

  def assignMappedXmlData(xmlData: String) = {
    """
   private def populateXml(xmlData:XmlData) : Unit = {
	try{
	  //XML Population is not handled at this time      
     
	}catch{
	  case e:Exception =>{
	    e.printStackTrace()
		throw e	    	
	  }
   	}
  }
"""
  }

  def assignJsonForArray(fname: String, typeImpl: String, msg: Message, typ: String): String = {
    var funcStr: String = ""

    val funcName = "fields(\"" + fname + "\")";
    if (msg.Fixed.toLowerCase().equals("true")) {
      funcStr = """
			if (map.contains("""" + fname + """")){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				""" + fname + """  = arrFld.map(v => """ + typeImpl + """(v.toString)).toArray
			} else """ + fname + """  = new """ + typ + """(0)
	    }
	      """
    } else if (msg.Fixed.toLowerCase().equals("false")) {
      funcStr = """
			if (map.contains("""" + fname + """" )){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				""" + funcName + """   = (-1, arrFld.map(v =>  {""" + typeImpl + """(v.toString) } ).toArray)
			}else 
				""" + funcName + """   = (-1, new """ + typ + """(0))
	    }
	      """
    }
    funcStr
  }

  def assignJsonForPrimArrayBuffer(fname: String, typeImpl: String, msg: Message, typ: String): String = {
    var funcStr: String = ""

    val funcName = "fields(\"" + fname + "\")";
    if (msg.Fixed.toLowerCase().equals("true")) {
      funcStr = """
			if (map.contains("""" + fname + """")){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				val arrFld = CollectionAsArrString(arr)
				 arrFld.map(v => {""" + fname + """  :+=""" + typeImpl + """(v.toString)})
			}else """ + fname + """  = new """ + typ + """(0)
	    }
	      """
    } else if (msg.Fixed.toLowerCase().equals("false")) {
      funcStr = """
			if (map.contains("""" + fname + """" )){
				val arr = map.getOrElse("""" + fname + """", null)
			if (arr != null) {
				var """ + fname + """  = new """ + typ + """
				val arrFld = CollectionAsArrString(arr)
				""" + funcName + """   = (-1, arrFld.map(v => {""" + fname + """  :+=""" + typeImpl + """(v.toString)}))
				}else 
				""" + funcName + """   = (-1, new """ + typ + """(0))
	    }
	      """
    }
    funcStr
  }

  def assignJsonForCntrArrayBuffer(fname: String, typeImpl: String) = {
    """
	 
	    if (map.getOrElse("""" + fname + """", null).isInstanceOf[List[tMap]])
	    	list = map.getOrElse("""" + fname + """", null).asInstanceOf[List[Map[String, Any]]]
        if (list != null) {
        	""" + fname + """++= list.map(item => {
        	val inputData = new JsonData(json.dataInput)
        	inputData.root_json = json.root_json
        	inputData.cur_json = Option(item)
        	val elem = new """ + typeImpl + """()
        	elem.populate(inputData)
        	elem
        	})
	    }
	    """
  }
  def assignJsonDataMessage(mName: String) = {
    """   
        val inputData = new JsonData(json.dataInput)
        inputData.root_json = json.root_json
        inputData.cur_json = Option(map.getOrElse("""" + mName + """", null))
	    """ + mName + """.populate(inputData)
	    """
  }

  def getArrayStr(mbrVar: String, classname: String): String = {

    "\t\tfor (i <- 0 until " + mbrVar + ".length) {\n" +
      "\t\t\tvar ctrVar: " + classname + " = i.asInstanceOf[" + classname + "]\n\t\t\t" +
      """try {
          		if (ctrVar != null)
          			ctrVar.populate(inputdata)
            } catch {
            	case e: Exception => {
            	e.printStackTrace()
            	throw e
            	}
            }
        }
    """
  }

  def mappedToStringForKeys() = {

    """
    private def toStringForKey(key: String): String = {
	  val field = fields.getOrElse(key, (-1, null))
	  if (field._2 == null) return ""
	  field._2.toString
	}
    """
  }
  // Default Array Buffer of message values in Mapped Messages
  def getAddMappedMsgsInConstructor(mappedMsgFieldsVar: String): String = {
    if (mappedMsgFieldsVar == null || mappedMsgFieldsVar.trim() == "") return ""
    else return """
      AddMsgsInConstructor

    private def AddMsgsInConstructor: Unit = {
      """ + mappedMsgFieldsVar + """
    }
      """

  }
  // Default Array Buffer of primitive values in Mapped Messages

  def getAddMappedArraysInConstructor(mappedArrayFieldsVar: String, mappedMsgFieldsArryBuffer: String): String = {
    if (mappedArrayFieldsVar == null || mappedArrayFieldsVar.trim() == "") return ""
    else return """
    AddArraysInConstructor

    private def AddArraysInConstructor: Unit = {
      """ + mappedArrayFieldsVar + """
      """ + mappedMsgFieldsArryBuffer + """
    }
      """

  }

  /**
   * For Mapped messages - converversion to Current obj
   *
   */

  def getConvertOldVertoNewVer() = {
    """
     oldObj.fields.foreach(field => {
         if(field._2._1 >= 0)
       
    	   fields(field._1) = (field._2._1, field._2._2);
     })
    
    """

  }

  /*
   * function to convert the old version to new version in desrializarion of messages especially when ArrayBuffer/Array of child messages or Child Message occurs
   */
  def getConvertOldVertoNewVer(convertStr: String, oldObj: String, newObj: Any): String = {
    var convertFuncStr: String = ""
    // if (prevObjExists) {
    if (oldObj != null && oldObj.toString.trim() != "") {
      if (convertStr != null && convertStr.trim() != "") {

        convertFuncStr = """
     def ConvertPrevToNewVerObj(oldObj : """ + oldObj + """) : Unit = {    
         if( oldObj != null){
           """ + convertStr + """
         }  
       }"""
        //    }
      }
    } else {
      convertFuncStr = """
   def ConvertPrevToNewVerObj(obj : Any) : Unit = { }
   """
    }

    convertFuncStr
  }

  def SerDeserStr = {
    """
    override def Serialize(dos: DataOutputStream) : Unit = { }
	override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = { }
    """
  }

  //create the serialized function in generated scala class 
  def getSerializedFunction(serStr: String): String = {
    var getSerFunc: String = ""

    if (serStr != null && serStr.trim() != "") {
      getSerFunc = """
    override def Serialize(dos: DataOutputStream) : Unit = {
        try {
    	   """ + serStr + """
    	} catch {
    		case e: Exception => {
    	    e.printStackTrace()
    	  }
        }
     } 
     """
    } else {
      getSerFunc = """ 
    } 
     
     """
    }
    getSerFunc
  }

  //create the deserialized function in generated scala class 

  def getPrevDeserStr(prevVerMsgObjstr: String, prevObjDeserStr: String, recompile: Boolean): String = {
    var preVerDeserStr: String = ""
    // if (recompile == false && prevVerMsgObjstr != null && prevVerMsgObjstr.trim() != "") {

    if (prevVerMsgObjstr != null && prevVerMsgObjstr.trim() != "") {
      val prevVerObjStr = "val prevVerObj = new %s()".format(prevVerMsgObjstr)
      preVerDeserStr = """
        if (prevVer < currentVer) {
                """ + prevVerObjStr + """ 
                prevVerObj.Deserialize(dis, mdResolver, loader, savedDataVersion)   
               """ + prevObjDeserStr + """ 
           
	     } else """
    }

    preVerDeserStr
  }

  def getDeserStr(deserStr: String, fixed: Boolean): String = {
    var deSer: String = ""

    if (deserStr != null && deserStr.trim() != "") {
      deSer = """
         if(prevVer == currentVer){  
              """ + deserStr + """
        } else throw new Exception("Current Message/Container Version "+currentVer+" should be greater than Previous Message Version " +prevVer + "." )
     """
    }
    deSer
  }

  def deSerializeStr(preVerDeserStr: String, deSer: String) = {

    """
    override def Deserialize(dis: DataInputStream, mdResolver: MdBaseResolveInfo, loader: java.lang.ClassLoader, savedDataVersion: String): Unit = {
	  try {
      	if (savedDataVersion == null || savedDataVersion.trim() == "")
        	throw new Exception("Please provide Data Version")
    
      	val prevVer = savedDataVersion.replaceAll("[.]", "").toLong
      	val currentVer = Version.replaceAll("[.]", "").toLong
      	""" + preVerDeserStr + """ 
      	""" + deSer + """ 
      	} catch {
      		case e: Exception => {
          		e.printStackTrace()
      		}
      	}
    } 
     """
  }

  def getDeserializedFunction(fixed: Boolean, deserStr: String, prevObjDeserStr: String, prevVerMsgObjstr: String, recompile: Boolean): String = {

    var getDeserFunc: String = ""
    var preVerDeserStr: String = ""
    var deSer: String = ""
    preVerDeserStr = getPrevDeserStr(prevVerMsgObjstr, prevObjDeserStr, recompile)
    deSer = getDeserStr(deserStr, fixed)

    if (deserStr != null && deserStr.trim() != "")
      getDeserFunc = deSerializeStr(preVerDeserStr, deSer)

    getDeserFunc
  }
  /// DeSerialize Base Msg Types for mapped Mapped 

  def MappedMsgDeserBaseTypes(baseTypesDeserialize: String) = {
    """
	  val desBaseTypes = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
	  //println("desBaseTypes "+desBaseTypes)
        for (i <- 0 until desBaseTypes) {
          val key = com.ligadata.BaseTypes.StringImpl.DeserializeFromDataInputStream(dis)
          val typIdx = com.ligadata.BaseTypes.IntImpl.DeserializeFromDataInputStream(dis)
          
	  	  typIdx match {
          	""" + baseTypesDeserialize + """
          	case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
          }
         
       }

   """
  }

  /// Serialize Base Msg Types for mapped Mapped 

  def MappedMsgSerializeBaseTypes(baseTypesSerialize: String) = {
    """
    private def SerializeBaseTypes(dos: DataOutputStream): Unit = {
  
    var cntCanSerialize: Int = 0
    fields.foreach(field => {
      if (field._2._1 >= 0)
        cntCanSerialize += 1
    })
    com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, cntCanSerialize)

    // Note:: fields has all base and other stuff
    fields.foreach(field => {
	  if (field._2._1 >= 0) {
      	val key = field._1.toLowerCase
      	com.ligadata.BaseTypes.StringImpl.SerializeIntoDataOutputStream(dos, key)
      	com.ligadata.BaseTypes.IntImpl.SerializeIntoDataOutputStream(dos, field._2._1)
      	field._2._1 match {
      	""" + baseTypesSerialize + """
   
      		case _ => {} // could be -1
      	}
      }
    })
   }
  """
  }

  //Serialize function of mapped maeesge

  def MappedMsgSerialize() = {
    """
    override def Serialize(dos: DataOutputStream): Unit = {
    try {
      // Base Stuff
      SerializeBaseTypes(dos)
       // Non Base Types
     SerializeNonBaseTypes(dos)
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }
    }
  }
  """
  }

  // Mapped Messages Serialization for Array of primitives
  def MappedMsgSerializeArrays(mappedMsgSerializeArray: String) = {
    """
    private def SerializeNonBaseTypes(dos: DataOutputStream): Unit = {
    """ + mappedMsgSerializeArray + """
    }
    """
  }

  def mappedPrevObjTypNotMatchDeserializedBuf(prevObjTypNotMatchDeserializedBuf: String) = {
    """
    private def getStringIdxFromPrevFldValue(oldTypName: Any, value: Any): (String, Int) = {
	  	var data: String = null    
	  	oldTypName match {
	  		""" + prevObjTypNotMatchDeserializedBuf + """
	  		case _ => { throw new Exception("Bad TypeIndex found") } // should not come here
	    }
	    return (data, typsStr.indexOf(oldTypName))
	  }
    """
  }

  //mapped messages - block of prevObj version < current Obj version check 

  def prevVerLessThanCurVerCheck(caseStmsts: String) = {
    var caseStr: String = ""
    if (caseStmsts != null && caseStmsts.trim() != "") {
      caseStr = """prevObjfield._1 match{
      		""" + caseStmsts + """
  			case _ => {
  				fields(prevObjfield._1) = (prevObjfield._2._1, prevObjfield._2._2)
  			}
  		  }
      	"""
    } else caseStr = "fields(prevObjfield._1) = (prevObjfield._2._1, prevObjfield._2._2)"

    """
      val prevTyps = prevVerObj.typsStr
      prevVerObj.fields.foreach(prevObjfield => {

      //Name and Base Types Match

      if (prevVerMatchKeys.contains(prevObjfield._1)) {
      	if(prevObjfield._2._1 >= 0){
           fields(prevObjfield._1) = (typsStr.indexOf(prevTyps(prevObjfield._2._1)), prevObjfield._2._2);
	  	}else {
           """ + caseStr + """
      	}
        } else if (prevVerTypesNotMatch.contains(prevObjfield._1)) { //Name Match and Base Types Not Match
            if (prevObjfield._2._1 >= 0) {
              val oldTypName = prevTyps(prevObjfield._2._1)
              val (data, newTypeIdx) = getStringIdxFromPrevFldValue(oldTypName, prevObjfield._2._2)
              val v1 = typs(newTypeIdx).Input(data)
              fields.put(prevObjfield._1, (newTypeIdx, v1))
            }

        } else if (!(prevVerMatchKeys.contains(prevObjfield._1) && prevVerTypesNotMatch.contains(prevObjfield._1))) { //////Extra Fields in Prev Ver Obj
            if (prevObjfield._2._1 >= 0) {
              val oldTypName = prevTyps(prevObjfield._2._1)
              val (data, index) = getStringIdxFromPrevFldValue(oldTypName, prevObjfield._2._2)

              val v1 = typs(0).Input(data)
              fields.put(prevObjfield._1, (0, v1))
            }
            else {
               fields.put(prevObjfield._1, (0, typs(0).Input(prevObjfield._2._2.asInstanceOf[String])))
            }
         }
       })
	 """
  }

}