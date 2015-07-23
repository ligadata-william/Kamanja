package com.ligadata.samples.models;

import com.ligadata.FatafatBase.*;
import com.ligadata.samples.messages.*;
import com.google.common.base.Optional;

public class JavaTestMdl extends ModelBase {

	static JavaTestMdlObj objSignleton = new JavaTestMdlObj();

	public ModelResultBase execute(boolean emitAllResults) {
		// Directly calling methods from Scala Singleton object. Not preferable
		// to use direct scala.
		CustAlertHistory custAlertHistory = CustAlertHistoryFactory.rddObject.getRecentOrNew();

		// Getting Java RDD Object and performing operations on that
		JavaRDDObject<CustAlertHistory> javaRddObj = CustAlertHistoryFactory.rddObject;
		Optional<CustAlertHistory> obj = javaRddObj.getRecent();

		if (obj.isPresent()) {

		} else {

		}

		return null;
	}

	public JavaTestMdl(ModelContext mdlContext) {
		super(mdlContext, objSignleton);
	}

	public static class JavaTestMdlObj implements ModelBaseObj {
		public boolean IsValidMessage(MessageContainerBase msg) {
			return (msg instanceof CustAlertHistory);
		}

		public ModelBase CreateNewModel(ModelContext mdlContext) {
			return new JavaTestMdl(mdlContext);
		}

		public String ModelName() {
			return "JavaTestMdl";
		}

		public String Version() {
			return "0.0.1";
		}

		public ModelResultBase CreateResultObject() {
			return new MappedModelResults();
		}
	}
}
