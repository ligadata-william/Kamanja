package com.ligadata.MetadataAPI

import org.apache.log4j._
import java.io.File
import java.util.Date
import java.util.Properties
import com.ligadata.Serialize._
import com.ligadata.fatafat.metadata._

object SecAuditImpl { 
   lazy val loggerName = this.getClass.getName
   lazy val logger = Logger.getLogger(loggerName) 
   lazy val serializer = SerializerManager.GetSerializer("kryo")
   private var authObj: SecurityAdapter = null
   private var auditObj: AuditAdapter = null
  
  /**
   * InitSecImpl  - 1. Create the Security Adapter class.  The class name and jar name containing
   *                the implementation of that class are specified in the CONFIG FILE.
   *                2. Create the Audit Adapter class, The class name and jar name containing
   *                the implementation of that class are specified in the CONFIG FILE.
   */
  def InitSecImpl: Unit = {
    logger.debug("Establishing connection to domain security server..")
    val classLoader: MetadataLoader = new MetadataLoader
    
    // Validate the Auth/Audit flags for valid input.
    if ((MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH") != null) &&
        (!MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES") &&
         !MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("NO"))) {
      throw new Exception("Invalid value for DO_AUTH detected.  Correct it and restart")
    }    
    if ((MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUDIT") != null) &&
        (!MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("YES") &&
         !MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("NO"))) {
      throw new Exception("Invalid value for DO_AUDIT detected.  Correct it and restart")
    } 
    
    
    // If already have one, use that!
    if (authObj == null && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH") != null) && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES"))) {
      createAuthObj(classLoader)
    }
    if (auditObj == null && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUDIT") != null) && (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUDIT").equalsIgnoreCase("YES"))) {
      createAuditObj(classLoader)
    }
  }
  
  
  /**
   * private method to instantiate an authObj
   */
  private def createAuthObj(classLoader : MetadataLoader): Unit = {
    // Load the location and name of the implementing class from the
    val implJarName = if (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SECURITY_IMPL_JAR") == null) "" else MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SECURITY_IMPL_JAR").trim
    val implClassName = if (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SECURITY_IMPL_CLASS") == null) "" else MetadataAPIImpl.GetMetadataAPIConfig.getProperty("SECURITY_IMPL_CLASS").trim
    logger.debug("Using "+implClassName+", from the "+implJarName+" jar file")
    if (implClassName == null) {
      logger.error("Security Adapter Class is not specified")
      return
    }

    // Add the Jarfile to the class loader
    loadJar(classLoader,implJarName)

    // All is good, create the new class
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[com.ligadata.fatafat.metadata.SecurityAdapter]]
    authObj = className.newInstance
    authObj.init
    logger.debug("Created class "+ className.getName)
  }
  
    
  /**
   * private method to instantiate an authObj
   */
  private def createAuditObj (classLoader : MetadataLoader): Unit = {
    // Load the location and name of the implementing class froms the
    val implJarName = if (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("AUDIT_IMPL_JAR") == null) "" else MetadataAPIImpl.GetMetadataAPIConfig.getProperty("AUDIT_IMPL_JAR").trim
    val implClassName = if (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("AUDIT_IMPL_CLASS") == null) "" else MetadataAPIImpl.GetMetadataAPIConfig.getProperty("AUDIT_IMPL_CLASS").trim
    logger.debug("Using "+implClassName+", from the "+implJarName+" jar file")
    if (implClassName == null) {
      logger.error("Audit Adapter Class is not specified")
      return
    }

    // Add the Jarfile to the class loader
    loadJar(classLoader,implJarName)

    // All is good, create the new class
    var className = Class.forName(implClassName, true, classLoader.loader).asInstanceOf[Class[com.ligadata.fatafat.metadata.AuditAdapter]]
    auditObj = className.newInstance
    auditObj.init
    logger.debug("Created class "+ className.getName)
  }
  
  /**
   * loadJar - load the specified jar into the classLoader
   */
  private def loadJar (classLoader : MetadataLoader, implJarName: String): Unit = {
    // Add the Jarfile to the class loader
    val jarPaths = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("JAR_PATHS").split(",").toSet
    val jarName = JarPathsUtils.GetValidJarFile(jarPaths, implJarName)
    val fl = new File(jarName)
    if (fl.exists) {
      try {
        classLoader.loader.addURL(fl.toURI().toURL())
        logger.debug("Jar " + implJarName.trim + " added to class path.")
        classLoader.loadedJars += fl.getPath()
      } catch {
          case e: Exception => {
            logger.error("Failed to add "+implJarName + " due to internal exception " + e.printStackTrace)
            return
          }
        }
    } else {
      logger.error("Unable to locate Jar '"+implJarName+"'")
      return
    }
  }
  
  /**
   * logAuditRec - Record an Audit event using the audit adapter.
   */
  def logAuditRec(userOrRole:Option[String], userPrivilege:Option[String], action:String, objectText:String, success:String, transactionId:String, objName:String) = {
    if( auditObj != null ){
      val aRec = new AuditRecord

      // If no userName is provided here, that means that somehow we are not running with Security but with Audit ON.
      var userName = "undefined"
      if( userOrRole != None ){
        userName = userOrRole.get
      }

      // If no priv is provided here, that means that somehow we are not running with Security but with Audit ON.
      var priv = "undefined"
      if( userPrivilege != None ){
        priv = userPrivilege.get
      }

      aRec.userOrRole = userName
      aRec.userPrivilege = priv
      aRec.actionTime = (new Date).getTime.toString
      aRec.action = action
      aRec.objectAccessed = objName
      aRec.success = success
      aRec.transactionId = transactionId
      aRec.notes  = objectText
      try{
        auditObj.addAuditRecord(aRec)
      } catch {
        case e: Exception => {
          throw new UpdateStoreFailedException("Failed to save audit record" + aRec.toString + ":" + e.getMessage())
        }
      }
    }
  }
  
  /**
   * getAuditRec - Get an audit record from the audit adapter.
   */
  def getAuditRec(startTime: Date, endTime: Date, userOrRole:String, action:String, objectAccessed:String) : String = {
    var apiResultStr = ""
    if( auditObj == null ){
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try{
      val recs = auditObj.getAuditRecord(startTime,endTime,userOrRole,action,objectAccessed)
      if( recs.length > 0 ){
        apiResultStr = JsonSerializer.SerializeAuditRecordsToJson(recs)
      }
      else{
        apiResultStr = "no audit records found "
      }
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString)
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }
  

  
  /**
   * checkAuth
   */
   def checkAuth (usrid:Option[String], password:Option[String], role: Option[String], privilige: String): Boolean = {
 
     var authParms: java.util.Properties = new Properties
     // Do we want to run AUTH?
     if ((MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH") == null) ||
         (MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH") != null && !MetadataAPIImpl.GetMetadataAPIConfig.getProperty("DO_AUTH").equalsIgnoreCase("YES"))) {
       return true
     }
     
     // check if the Auth object exists
     if (authObj == null) return false
 
     // Run the Auth - if userId is supplied, defer to that.
     if ((usrid == None) && (role == None)) return false
 
     if (usrid != None) authParms.setProperty("userid",usrid.get.asInstanceOf[String])
     if (password != None) authParms.setProperty("password", password.get.asInstanceOf[String])
     if (role != None) authParms.setProperty("role", role.get.asInstanceOf[String])
     authParms.setProperty("privilige",privilige)
 
     return authObj.performAuth(authParms)
   } 
  
  /**
   * getPrivilegeName
   */
  def getPrivilegeName(op: String, objName: String): String = {
    // check if the Auth object exists
    logger.debug("op => " + op)
    logger.debug("objName => " + objName)
    if (authObj == null) return ""
    return authObj.getPrivilegeName (op,objName)
  }  
  
  
     def getAuditRec(filterParameters: Array[String]) : String = {
    var apiResultStr = ""
    if( auditObj == null ){
      apiResultStr = "no audit records found "
      return apiResultStr
    }
    try{
      var audit_interval = 10
      var ai = MetadataAPIImpl.GetMetadataAPIConfig.getProperty("AUDIT_INTERVAL")
      if( ai != null ){ audit_interval = ai.toInt }
      var startTime:Date = new Date((new Date).getTime() - audit_interval * 60000)
      var endTime:Date = new Date()
      var userOrRole:String = null
      var action:String = null
      var objectAccessed:String = null

      if (filterParameters != null ){
        val paramCnt = filterParameters.size
        paramCnt match {
          case 1 => {startTime = MetadataUtils.parseDateStr(filterParameters(0))}
          case 2 => {
            startTime = MetadataUtils.parseDateStr(filterParameters(0))
            endTime = MetadataUtils.parseDateStr(filterParameters(1))
          }
          case 3 => {
            startTime = MetadataUtils.parseDateStr(filterParameters(0))
            endTime = MetadataUtils.parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
          }
          case 4 => {
            startTime = MetadataUtils.parseDateStr(filterParameters(0))
            endTime = MetadataUtils.parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
            action = filterParameters(3)
          }
          case 5 => {
            startTime = MetadataUtils.parseDateStr(filterParameters(0))
            endTime =MetadataUtils.parseDateStr(filterParameters(1))
            userOrRole = filterParameters(2)
            action = filterParameters(3)
            objectAccessed = filterParameters(4)
          }
        }
      }
      else{
        logger.debug("filterParameters is null")
      }
      apiResultStr = getAuditRec(startTime,endTime,userOrRole,action,objectAccessed)
    } catch {
      case e: Exception => {
        var apiResult = new ApiResult(ErrorCodeConstants.Failure, "Failed to fetch all the audit objects:", null, "Error :" + e.toString )
        apiResultStr = apiResult.toString()
      }
    }
    logger.debug(apiResultStr)
    apiResultStr
  }
     
   def shutdownAuditAdapter(): Unit = {
    if (auditObj != null) auditObj.Shutdown
  }


}