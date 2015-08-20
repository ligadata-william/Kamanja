package com.ligadata.kamanja.metadata

import com.ligadata.Exceptions.InvalidArgumentException

/**
 * @author danielkozin
 */
object Utils {
  
  /**
   * parseNameToken - Parses the fully qualified Kamanja Object name (with version).
   * @parm - name: String
   * @return - (ObjectNameSpace: String, ObjectName String, ObjectVerion String)
   */
  def parseNameToken(name: String): (String, String, String) = {
    var msgTokens = name.split("\\.")
    if (msgTokens.length < 3) throw new InvalidArgumentException("Invalid Metadata Object Name, Unable to parse Namespace.Name.Version")
    return (msgTokens.dropRight(2).mkString("."), msgTokens(msgTokens.length-2), msgTokens(msgTokens.length-1))
  } 
  
  /**
   * parseNameToken - Parses the fully qualified Kamanja Object name (with version).
   * @parm - name: String
   * @return - (ObjectNameSpace: String, ObjectName String, ObjectVerion String)
   */
  def parseNameTokenNoVersion(name: String): (String, String) = {
    var msgTokens = name.split("\\.")
    if (msgTokens.length < 2) throw new InvalidArgumentException("Invalid Metadata Object Name, Unable to parse Namespace.Name.Version")
    return (msgTokens.dropRight(1).mkString("."), msgTokens(msgTokens.length-1))
  }   
  
  def makeFullName (nameSpace: String, name: String): String = {nameSpace+"."+name}
  
}