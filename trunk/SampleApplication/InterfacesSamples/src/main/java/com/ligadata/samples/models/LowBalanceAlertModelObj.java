package com.ligadata.samples.models;

import com.ligadata.FatafatBase.*;
import com.ligadata.samples.messages.*;

public class LowBalanceAlertModelObj implements ModelBaseObj {
	
	public boolean IsValidMessage(MessageContainerBase msg) {
		return (msg instanceof CustTransaction);
	}
	
	public ModelBase CreateNewModel(ModelContext txnContext) {
		return new LowBalanceAlertModel(txnContext);
	}

	public String ModelName() {
		return "JavaTestMdl";
	}

	public String Version() {
		return "0.0.1";
	}

}