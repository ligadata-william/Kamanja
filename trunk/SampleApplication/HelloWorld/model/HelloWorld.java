package com.ligadata.kamanja.financial;

import com.google.common.base.Optional;
import com.ligadata.KamanjaBase.*;
import System.*;

public class HelloWorldModel extends ModelBase {
	static HelloWorldModelObj objSingleton = new HelloWorldModelObj();
	ModelContext mdlCntxt;

	public HelloWorldModel(ModelContext mdlContext) {
    	super(mdlContext, objSingleton);
    	mdlCntxt = mdlContext;
    }


	@Override
	public ModelBaseObj factory() {
		// TODO Auto-generated method stub
		return objSingleton;
	}

	@Override
	public ModelContext modelContext() {
		// TODO Auto-generated method stub
		return mdlCntxt;
	}

	public ModelResultBase execute(boolean emitAllResults) {
    	/*
		System.out.println("inside model");
    	GlobalPreferences gPref = GlobalPreferencesFactory.rddObject.getRecentOrNew(new String[]{"PrefType"});  //(new String[]{"Type1"});
    	//System.out.println(gPref.);
    	CustPreferences cPref = (CustPreferences) CustPreferences.getRecentOrNew();
    	cPref.Save();
    	//GlobalPreferences gPref = GlobalPreferences.toJavaRDDObject().getRecentOrNew(new String[]{"Type1"});
    	//CustPreferences cPref = CustPreferences.toJavaRDDObject().getRecentOrNew();
    	System.out.println("Can persist:"+cPref.CanPersist());

    	if(cPref.minbalancealertoptout())
    	{
    		System.out.println("got minimum balance");
    		System.out.println(cPref.minbalancealertoptout());
    		return null;
    	}

    	RddDate curDtTmInMs = RddDate.currentGmtDateTime();
    	CustAlertHistory alertHistory = CustAlertHistory.toJavaRDDObject().getRecentOrNew(new String[]{"custId"});


    	if(curDtTmInMs.timeDiffInHrs(new RddDate(alertHistory.alertdttminms())) < gPref.minalertdurationinhrs())
    	{
    		return null;
    	}

    	TransactionMsg rcntTxn = (TransactionMsg) this.mdlCntxt.msg();

    	 if (rcntTxn.balance() >= gPref.minalertbalance())
    	      return null;
		*/
		msg1 helloWorld = (msg1) this.mdlCntxt.msg();
		if(helloWorld.score()!=1)
			return null;

        Result[] actualResult = {new Result("Id",helloWorld.id()) , new Result("Name",helloWorld.Name()), new Result("Score",helloWorld.score())};
        return new MappedModelResults().withResults(actualResult);
  }

    /**
     * @param inTxnContext
     */




    public static class HelloWorldModelObj implements ModelBaseObj {
		public boolean IsValidMessage(MessageContainerBase msg) {
			return (msg instanceof msg1);
		}

		public ModelBase CreateNewModel(ModelContext mdlContext) {
			return new HelloWorldModel(mdlContext);
		}

		public String ModelName() {
			return "HelloWorldModel";
		}

		public String Version() {
			return "0.0.1";
		}

		public ModelResultBase CreateResultObject() {
			return new MappedModelResults();
		}
	}

}










