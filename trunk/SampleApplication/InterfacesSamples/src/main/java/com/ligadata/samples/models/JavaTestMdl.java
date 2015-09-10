package com.ligadata.samples.models;

import com.ligadata.KamanjaBase.*;
import com.ligadata.samples.messages.*;
import com.google.common.base.Optional;

public class JavaTestMdl extends ModelBase {

	static JavaTestMdlObj objSignleton = new JavaTestMdlObj();

	private final ModelContext modelContext;

	@Override
	public ModelContext modelContext() {
		return modelContext;
	}

	@Override
	public String modelName() {
		return objSignleton.modelName();
	}

	@Override
	public String version() {
		return objSignleton.version();
	}

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

	public JavaTestMdl(ModelContext modelContext) {
		this.modelContext = modelContext;
	}

	public static class JavaTestMdlObj implements ModelFactory {
		public boolean isValidMessage(MessageContainerBase msg) {
			return (msg instanceof CustAlertHistory);
		}

		public ModelBase createNewModel(ModelContext mdlContext) {
			return new JavaTestMdl(mdlContext);
		}

		public String modelName() {
			return "JavaTestMdl";
		}

		public String version() {
			return "0.0.1";
		}

		public ModelResultBase createResultObject() {
			return new MappedModelResults();
		}
	}
}