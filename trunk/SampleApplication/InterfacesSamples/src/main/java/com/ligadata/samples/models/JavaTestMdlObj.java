package com.ligadata.samples.models;

import com.ligadata.FatafatBase.*;
import com.ligadata.samples.messages.*;

public class JavaTestMdlObj implements ModelBaseObj {
	public boolean IsValidMessage(MessageContainerBase msg) {
		return (msg instanceof CustAlertHistory);
	}

	public ModelBase CreateNewModel(TransactionContext txnContext) {
		return new JavaTestMdl(txnContext);
	}

	public String ModelName() {
		return "JavaTestMdl";
	}

	public String Version() {
		return "0.0.1";
	}
}
