package com.ligadata.olep.metadata

import java.util.Properties
import java.util.Date

/**
 * This trait must be implemented by the actual Security Implementation for Fatafat.  All Metadata access methods will 
 * call the preformAuth call prior to executing the code.
 */
trait SecurityAdapter {
  var adapterProperties: Map[String,Any] = null
  def setProperties(props: Map[String,Any]): Unit = {adapterProperties = props}
  
  // Implement these methods
  
  // call off to the appropriate engine to see if this user is allowed to proceed
  def performAuth(secParms: java.util.Properties): Boolean
  
  // get the name of the READ/WRITE privilege name
  def getPrivilegeName(operation: String, objectName: String): String
  
  // Set the desired properties for this adapter
  def init:Unit
}

/**
 * This trait must be implemented by the actual Audit Implementation for Fatafat.  All Metadata access methods will
 * call the ADD method when
 */
trait AuditAdapter {
  var adapterProperties: Map[String,String] = null
  def Shutdown() = {}
  
  // Implement these methods
    
  // Add an Audit Record to an appropriate system
  def addAuditRecord(rec: AuditRecord)
  
  // Get an audit record from an appropriate system.
  def get(startTime: Date, endTime: Date, userOrRole: String, action: String, objectAccessed: String): Array[AuditRecord]
  
  // Set the desired properties for this adapter
  def init: Unit
}

object AuditConstants {
  // Audit Actions
  val GETOBJECT = "getObject"
  val GETKEYS = "getKeys"
  val UPDATEOBJECT = "updateObject"
  val INSERTOBJECT = "insertObject"
  val DELETEOBJECT = "deleteObject"
  val ACTIVATEOBJECT = "activateObject"
  val DEACTIVATEOBJECT = "deactivateObject"
  val REMOVECONFIG = "removeConfig"
  val INSERTCONFIG = "insertConfig"
  val UPDATECONFIG = "updateConfig"
  val GETCONFIG = "getConfig"
  val INSERTJAR = "uploadJar"
  
  // Objects
  val MESSAGE = "Message"
  val MODEL = "Model"
  val CONTAINER = "Container"
  val FUNCTION = "Function"
  val CONCEPT = "Concept"
  val TYPE = "Type"
  val OBJECT = "object"
  val CLUSTERID = "ClusterId"
  val CONFIG = "clusterConfiguration"
  val JAR = "jarFile"
  
  // Priviliges
  val READ = "read"
  val WRITE = "write"
  
  // Results
  val FAIL = "Access Denied"
  val SUCCESS = "Access Granted"
      
}

