package main.scala.com.ligadata.MetadataAPI.Utility


/**
 * Created by dhaval on 8/7/15.
 */
object Action extends Enumeration {
  type Action = Value
  //message manangemen
  val ADDMESSAGE=Value("addmessage")
  val UPDATEMESSAGE=Value("updatemessage")
  val GETALLMESSAGES=Value("getallmessages")
  val REMOVEMESSAGE=Value("removemessage")
  //output message management
  val ADDOUTPUTMESSAGE=Value("addoutputmessage")
  val UPDATEOUTPUTMESSAGE=Value("updateoutputmessage")
  val REMOVEOUTPUTMESSAGE=Value("removeoutputmessage")
  val GETALLOUTPUTMESSAGES=Value("getalloutputmessages")
  //model management
  val ADDMODELPMMML=Value("addmodelpmml")
  val ADDMODELSCALA=Value("addmodelscala")
  val ADDMODELJAVA=Value("addmodeljava")
  val REMOVEMODEL=Value("removemodel")
  val ACTIVATEMODEL=Value("activatemodel")
  val DEACTIVATEMODEL=Value("deactivatemodel")
  val UPDATEMODEL=Value("updatemodel")
  val GETALLMODELS=Value("getallmodels")
  val GETMODEL=Value("getmodel")
//container management
  val ADDCONTAINER = Value("addcontainer")
  val UPDATECONTAINER = Value("updatecontainer")
  val GETCONTAINER = Value("getcontainer")
  val GETALLCONTAINERS= Value("getallcontainers")
  val REMOVECONTAINER= Value("removecontainer")

}

