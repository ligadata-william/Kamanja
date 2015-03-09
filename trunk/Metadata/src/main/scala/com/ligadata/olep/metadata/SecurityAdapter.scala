package com.ligadata.olep.metadata
import java.util.Properties

/**
 * This train must be implemented by the Fatafat users.  All Metadata access methods will 
 * call the preformAuth call prior to executing the code.
 */
trait SecurityAdapter {
  var adapterProperties: Map[String,Any] = null
  def setProperties(props: Map[String,Any]): Unit = {adapterProperties = props}
  
  // Implement this method
  def performAuth(secParms: java.util.Properties): Boolean
  
  // get the name of the READ privilege name
  def getPrivilegeName(operation: String, objectName: String): String
}
