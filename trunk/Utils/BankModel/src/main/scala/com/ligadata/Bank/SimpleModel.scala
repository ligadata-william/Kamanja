package com.ligadata.Bank

import com.ligadata.FatafatBase.{ModelBase,ModelBaseObj,MessageContainerBase,EnvContext}
//import com.ligadata.messagescontainers._

/**
 * 
 */
object SimpleModel extends ModelBaseObj {
  
   val validMessages = Array("System.BankCustomerCodes")
   def IsValidMessage(msg: MessageContainerBase): Boolean = {
     validMessages.filter( m => m.toLowerCase == msg.FullName.toLowerCase).size > 0
   }

   def CreateNewModel(tempTransId: Long, gCtx: EnvContext, msg: MessageContainerBase, tenantId: String): ModelBase = {
     new SimpleModel(gCtx, msg, getModelName, getVersion, tenantId, tempTransId)
   }   
   def getModelName: String = { "System.SimpleModel"}  
   override def getVersion: String = {"1000011"}
   
   
   def main(args : Array[String]): Unit = {   
     var msg: System_BankCustomerCodes_1000001_1431110308663 = new System_BankCustomerCodes_1000001_1431110308663
     msg.feetype = "criminal"
     msg.feeammount = 2
     
     var myModel: ModelBase = SimpleModel.CreateNewModel(50,null, msg,"test,")
     myModel.execute(true)
   }
}

/**
 * 
 */
class SimpleModel (val gCtx : com.ligadata.FatafatBase.EnvContext, val basemsg : MessageContainerBase,
                   val modelName:String, val modelVersion:String, val tenantId: String, val tempTransId: Long) extends ModelBase {
    override def getModelName : String = SimpleModel.getModelName 
    override def getVersion : String = SimpleModel.getVersion
    override def getTenantId : String = tenantId
    override def getTempTransId: Long = tempTransId
    private val msg = basemsg.asInstanceOf[System_BankCustomerCodes_1000001_1431110308663]
    
    override def execute(emitAllResults : Boolean) : com.ligadata.FatafatBase.ModelResult = {
      doRule
      prepareResults(emitAllResults)
      null
    }  
    private def doRule: Unit = {
      println(" Executing Simple Model Rule for message " + msg.feetype + " is "+ msg.feeammount)
    }  
    private def prepareResults(emitAll: Boolean): Unit = {} 
}