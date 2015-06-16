package com.ligadata.samples.models;

import com.ligadata.FatafatBase.*;
import com.ligadata.samples.messages.*;
import com.google.common.base.Optional;


public class LowBalanceAlertModel extends ModelBase { 
	
	static LowBalanceAlertModelObj objSignleton = new LowBalanceAlertModelObj();
	
    private TransactionContext txnContext = null;
    
    // Collections/messages to be used in this model
    private GlobalPreferences gPref = null;
    private CustPreferences pref = null;
    private com.google.common.base.Optional<CustTransaction> rcntTxnOp = com.google.common.base.Optional.absent();
    private RddDate curDt = RddDate.currentDateTime();
    private CustAlertHistory alertHistory = null;
    
    public ModelResult execute(boolean emitAllResults) {
    
        // First check the preferences and decide whether to continue or not
    	gPref = GlobalPreferences.getRecentOrNew();
        pref = CustPreferences.getRecentOrNew();
        
        if (pref.minBalanceAlertOptout() == false)
          return null;

        // Check if at least min number of hours elapsed since last alert  
        alertHistory = CustAlertHistory.getRecentOrNew();
        
        if (curDt.timeDiffInHrs(alertHistory.alertDt()) < gPref.minAlertDurationInHrs())
          return null;
        
        // Getting Java RDD Object and performing operations on that
        rcntTxnOp = CustTransaction$.MODULE$.toJavaRDDObject().getRecent();
        if (!rcntTxnOp.isPresent() || rcntTxnOp.get().balance() > gPref.minAlertBalance()) {
          return null; 	
        }

        // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
        CustAlertHistory.build().withAlertDt(curDt).withAlertType("lowBalanceAlert").save();
        
        // ... Prepare results here ... need to populate result object with appropriate attributes
        return ModelResult.builder().withResult(new LowBalanceAlertResult(txnContext)).build(); 
    }
	
    /**
     * @param inTxnContext
     */
    public LowBalanceAlertModel (TransactionContext inTxnContext) {
    	super(new ModelContext(inTxnContext), objSignleton);
    }

    /**
      * 
      * @param msg
      * @return
      */	 
    public static boolean IsValidMessage(MessageContainerBase msg) {
  	  return (msg instanceof CustTransaction);
    }
       
       
    /**
    *  
    * @param txnContext
    * @return
    */
    public static LowBalanceAlertModel CreateNewModel(TransactionContext txnContext) {
      return new LowBalanceAlertModel(txnContext);  	  
    }
 
    /**
     * 
     * @return
     */
	public static String getModelName() {
		return "LowBalanceAlertModel";
	}

	/**
	 * 
	 * @return
	 */
	public static String getVersion() {
		return "0.0.1";
	}
	
	

    // This is the object that needs to be implemented to 
    class LowBalanceAlertResult {
    	private TransactionContext cxt;
    	public LowBalanceAlertResult(TransactionContext inTxnContext) {
          cxt = inTxnContext;
    	}
    }

}


