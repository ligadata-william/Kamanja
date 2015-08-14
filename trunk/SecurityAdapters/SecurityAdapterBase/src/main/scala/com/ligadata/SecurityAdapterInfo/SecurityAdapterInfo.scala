
package com.ligadata.SecurityAdapterInfo

import java.util.Properties
import java.util.Date

/**
 * This trait must be implemented by the actual Security Implementation for Kamanja.  All Metadata access methods will 
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
