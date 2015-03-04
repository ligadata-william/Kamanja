// An early implementation of authentication module using Apache-Shiro
// We have lot more work to do.
package com.ligadata.Security

import org.apache.shiro.SecurityUtils
import org.apache.shiro.authc._
import org.apache.shiro.config.IniSecurityManagerFactory
import org.apache.shiro.mgt.SecurityManager
import org.apache.shiro.session.Session
import org.apache.shiro.subject.Subject
import org.apache.shiro.util.Factory
import org.apache.log4j._

import com.ligadata.olep.metadata.SecurityAdapter
import java.util.Properties

class SimpleApacheShiroAdapter extends SecurityAdapter{

  override def performAuth(secParams: java.util.Properties): Boolean = {

    val loggerName = this.getClass.getName
    val log = Logger.getLogger(loggerName)
    log.setLevel(Level.TRACE);
    
    val factory = new IniSecurityManagerFactory("classpath:shiro.ini");
    val securityManager = factory.getInstance();
    SecurityUtils.setSecurityManager(securityManager);

    val username = secParams.getProperty("userid")
    if( username == null ){
      log.error("userid is not supplied: unable to authenticate")
      return false;
    }

    val password = secParams.getProperty("password")
    if( username == null ){
      log.error("password is not supplied: unable to authenticate")
      return false;
    }

    val role = secParams.getProperty("cert")
    val privilege = secParams.getProperty("action")

    if( role == null && privilege == null ){
      log.error("Either role or privilege must be supplied: unable to authenticate")
      return false;
    }

    // get the currently executing user:
    val currentUser = SecurityUtils.getSubject();

    // let's login the current user so we can check against roles and permissions:
    var authenticated = false;
    if (!currentUser.isAuthenticated()) {
      val token = new UsernamePasswordToken(username,password);
      token.setRememberMe(true);
      try {
        currentUser.login(token);
	      authenticated = true;
      } catch {
	      case uae:UnknownAccountException => {
          log.error("There is no user with username of " + token.getPrincipal());
          return false
	      } 
	      case ice:IncorrectCredentialsException => {
          log.error("Password for account " + token.getPrincipal() + " was incorrect!");
          return false
	      } 
	      case lae:LockedAccountException => {
          log.error("The account for username " + token.getPrincipal() + " is locked.  " +
                   "Please contact your administrator to unlock it.");
          return false
	      }
	      // ... catch more exceptions here, maybe custom ones specific to your application?
	      case ae: AuthenticationException => {
	        ae.printStackTrace()
	        log.error("Unexpected authorization exception " + ae.getMessage())
          return false
        }
      }
    } else {
      authenticated = true
    }

    if (authenticated == true ){
      //say who they are:
      //print their identifying principal (in this case, a username):
      log.trace("User [" + currentUser.getPrincipal() + "] authenticated successfully... checking authorization");

      //test a role:
      if( role != null ){
	      if (currentUser.hasRole(role)) {
	        log.debug("The role " + role + " is authorized !");
	      } else {
	        log.debug("The role " + role + " is not authorized !");
	        return false
	      }
      }
      
      //test a typed permission (not instance-level)
      if( privilege != null ){
	      if (currentUser.isPermitted(privilege)) {
	        log.debug("The privilege " + privilege + " is authorized ")
	      } else {
	        log.debug("The privilege " + privilege + " is not authorized ")
	        return false
	      }
      }
    }

    return true
  }
}
