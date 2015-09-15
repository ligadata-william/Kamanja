/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.kamanja.financial;

import com.ligadata.KamanjaBase.*;
import System.*;

public class LowBalanceAlertModel extends ModelBase {
	static LowBalanceAlertModelObj objSingleton = new LowBalanceAlertModelObj();
	ModelContext mdlCntxt;
	
	public LowBalanceAlertModel(ModelContext mdlContext) {
    	super(mdlContext, objSingleton);
    	mdlCntxt = mdlContext;
    }

	
	@Override
	public ModelFactory modelBaseObject() {
		// TODO Auto-generated method stub
		return objSingleton;
	}

	@Override
	public ModelContext modelContext() {
		// TODO Auto-generated method stub
		return mdlCntxt;
		
		
	}

	public ModelResultBase execute(boolean emitAllResults) {
    	
    	GlobalPreferences gPref = (GlobalPreferences) GlobalPreferences.getRecentOrNew(new String[]{"Type1"});  //(new String[]{"Type1"});
    	
    	CustPreferences cPref = (CustPreferences) CustPreferences.getRecentOrNew();
    	
    	
    	
    	if(cPref.minbalancealertoptout())
    	{
    		return null;
    	}
    	
    	RddDate curDtTmInMs = RddDate.currentGmtDateTime();
    	CustAlertHistory alertHistory = (CustAlertHistory) CustAlertHistory.getRecentOrNew();
    	
    	if(curDtTmInMs.timeDiffInHrs(new RddDate(alertHistory.alertdttminms())) < gPref.minalertdurationinhrs())
    	{
    		return null;
    	}
    	
    	
    	
    	
    	TransactionMsg rcntTxn = (TransactionMsg) this.modelContext().msg();
   
    	 if (rcntTxn.balance() >= gPref.minalertbalance())
    	      return null;
		
    	    long curTmInMs = curDtTmInMs.getDateTimeInMs();
    	    	    // create new alert history record and persist (if policy is to keep only one, this will replace existing one)
    	    	    CustAlertHistory.build().withalertdttminms(curTmInMs).withalerttype("lowbalancealert").Save();
    	
    	
        Result[] actualResult = {new Result("Customer ID",rcntTxn.custid()),
        						 new Result("Branch ID",rcntTxn.branchid()),
        						 new Result("Account No.",rcntTxn.accno()),
        						 new Result("Current Balance",rcntTxn.balance()),
        						 new Result("Alert Type","lowbalancealert"),
        						 new Result("Trigger Time",new RddDate(curTmInMs).toString())
        };
        return new MappedModelResults().withResults(actualResult);
  }

    /**
     * @param inTxnContext
     */ 
    
    
    public boolean minBalanceAlertOptout(JavaRDD<CustPreferences> pref)
    {
    	
    	return false;
    }
	
    public static class LowBalanceAlertModelObj implements ModelFactory {
		public boolean isValidMessage(MessageContainerBase msg) {
			return (msg instanceof TransactionMsg);
		}

		public ModelBase createNewModel(ModelContext mdlContext) {
			return new LowBalanceAlertModel(mdlContext);
		}

		public String modelName() {
			return "LowBalanceAlert";
		}

		public String version() {
			return "0.0.1";
		}
		
		public ModelResultBase createResultObject() {
			return new MappedModelResults();
		}
	}

}
	
	
	
	
	
	
	
	


