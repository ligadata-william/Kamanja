package main.scala.com.ligadata.MetadataAPI.Utility


/**
 * Created by dhaval on 8/7/15.
 */
object Action extends Enumeration {
  type Action = Value
  val ADDMESSAGE=Value("addmessage")
  val UPDATEMESSAGE=Value("updatemessage")
  val GETALLMESSAGES=Value("getallmessages")
  val REMOVEMESSAGE=Value("removemessage")
  val ADDMODELPMMML=Value("addmodelpmml")
  val ADDMODELSCALA=Value("addmodelscala")
  val ADDMODELJAVA=Value("addmodeljava")
  val REMOVEMODEL=Value("removemodel")
  val ACTIVATEMODEL=Value("activatemodel")
  val DEACTIVATEMODEL=Value("deactivatemodel")
  val UPDATEMODEL=Value("updatemodel")
  val GETALLMODELS=Value("getallmodels")
  val GETMODEL=Value("getmodel")
  val ADDOUTPUTMESSAGE=Value("addoutputmessage")
  val UPDATEOUTPUTMESSAGE=Value("updateoutputmessage")
  val REMOVEOUTPUTMESSAGE=Value("removeoutputmessage")
  val GETALLOUTPUTMESSAGES=Value("getalloutputmessages")
}

