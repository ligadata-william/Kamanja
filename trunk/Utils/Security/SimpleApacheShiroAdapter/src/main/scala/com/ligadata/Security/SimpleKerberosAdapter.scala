package com.ligadata.Security

import com.ligadata.fatafat.metadata.SecurityAdapter
import com.sun.security.auth.callback.TextCallbackHandler
import javax.security.auth.login._
import javax.security.auth.Subject;
import java.security._
import javax.security.auth.callback._;
import javax.security.auth.kerberos._

object SimpleKerberosAdapter {
    def main(args: Array[String]): Unit = {
    val ka = new SimpleKerberosAdapter
    var secParms: java.util.Properties = new java.util.Properties()
    
    secParms.setProperty("userid", "test2/ligadata.com@LIGADATA.COM")
    secParms.setProperty("password", "ligadata123")
    ka.performAuth(secParms) 
  }
}


class SimpleKerberosAdapter extends SecurityAdapter {
  
  /**
   * 
   */
  def performAuth(secParams: java.util.Properties): Boolean = {
    
    var mysubject: Subject = new Subject
    var lc: LoginContext = null
    
    val username = secParams.getProperty("userid")
    val password = secParams.getProperty("password")

    try {
       lc = new LoginContext("ligadataLoginContext", mysubject, new MyCallbackHandler);
    } catch {
        case uae: Exception => {
          return false
        }
    }
    
    try {
       lc.login();
    } catch{
      case e: LoginException => {return false}
    } 
    
    return true
  }
  
  /**
   * Simple thing for here.....  This impl treats all request as READ/WRITE
   */
  override def getPrivilegeName (op: String, objectName: String): String = {
    if (op.equalsIgnoreCase("get")) {
      return "read" 
    } else {
      return "write"
    }
  }
  
  /**
   * 
   */
  def init:Unit = {
    
  }
  class MyCallbackHandler extends CallbackHandler {
   def handle(callbacks: Array[Callback]): Unit = {
      callbacks.foreach(callback => {
        callback match {
          case nc:NameCallback => {
            nc.setName("name")
          }
          case pc:PasswordCallback => {
            pc.setPassword("blah".toCharArray())
          }
        }
      })   
    }
  } 

}
