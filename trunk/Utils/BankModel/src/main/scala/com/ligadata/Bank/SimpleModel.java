package com.ligadata.Bank;
import com.ligadata.FatafatBase.*;


/**
 * 
 * @author dan
 *
 */
public class SimpleModel implements ModelBase {

	private MessageContainerBase msg;
	private com.ligadata.FatafatBase.EnvContext gCtx;
	private String modelName;
	private String modelVersion;
	private String tenantId;
	private long tempTransId;

	public SimpleModel (com.ligadata.FatafatBase.EnvContext pgCtx,
			MessageContainerBase basemsg, 
            String pmName,
            String pmVersion, 
            String pTenantId,
            Long pTempTransId) {
        msg = basemsg;
        gCtx = pgCtx;
        modelName = pmName;
        modelVersion = pmVersion;
        tenantId = pTenantId;
        tempTransId = pTempTransId;	
	}
	

	@Override
	@LigadataModelAnnotations(
			dependencies={"shit1","shit2"},
			modelFullName="System.SimpleModel",
			emitResults="true"
	)
	public ModelResult execute(boolean emitResults) {	
		/**************************************************/
		/**************************************************/
		/**************************************************/
		/************Rules logic goes here*****************/
		/**************************************************/
		/**************************************************/
		/**************************************************/
	
		return null;
	}

	@Override
	public String getModelName() {
		return modelName;	// Model Name
	}
	
	@Override
	public String getVersion() {
		return modelVersion;	// Model Name
	}

	@Override
	public String getTenantId() {
		return tenantId;	// Model Name
	}

	@Override
	public long getTempTransId() {
		return tempTransId;	// Model Name
	}

	@Override
	public EnvContext gCtx() {
		return gCtx;
	}

	@Override
	public MessageContainerBase msg() {
		return msg;
	}

	@Override
	public String modelName() {
		return modelName;
	}

	@Override
	public String modelVersion() {
		return modelVersion;
	}

	@Override
	public String tenantId() {
		return tenantId;
	}

	@Override
	public long tempTransId() {
		return tempTransId;
	}
	
	public static class SimpleModelObj implements ModelBaseObj {

		private static String[] validMessages = {"System.BankCustomerCodes"}; 
		private static String mName = "System.SimpleModel";
		private static String version = "1000011";
		/**
		 * IsValidMessage - required static method... if this method returns a true, then the Fatafat
		 *                  engine will create an instance of this ModelClass and execute it with the 
		 *                  message that is the parameter here.
		 * @param msg - Message received by the engine from an Input Adapter
		 * @return boolean - TRUE if msg applies to this model.
		 */
		@Override
		public boolean IsValidMessage(MessageContainerBase msg) {
			for(int i = 0; i < validMessages.length; i++) {
			    if (msg.FullName().compareToIgnoreCase(validMessages[i]) == 0)
			    	return true;
			}
			return false;
		}

		@Override
		public ModelBase CreateNewModel (long tempTransId,
				                                com.ligadata.FatafatBase.EnvContext gCtx,
				                                MessageContainerBase msg,
				                                String tenantId) {
			 return new SimpleModel(gCtx, msg, getModelName(), getVersion(), tenantId, tempTransId);
		}


		@Override
		public String getModelName() {
			return mName;
		}

		@Override
		public String getVersion() {
			return version;
		}
	}
}
