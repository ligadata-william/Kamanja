package com.ligadata.samples.models;

import com.ligadata.FatafatBase.*;
import com.ligadata.samples.messages.*;

public class LowBalanceAlertModel2Obj implements ModelBaseObj {
	
	public boolean IsValidMessage(MessageContainerBase msg) {
		return (msg instanceof CustTransaction);
	}
	
	public ModelBase CreateNewModel(ModelContext txnContext) {
		return new LowBalanceAlertModel2(txnContext);
	}

	public String ModelName() {
		return "LowBalanceAlertModel2Obj";
	}

	public String Version() {
		return "0.0.1";
	}
}
