package com.ligadata.samples.messages;

import com.ligadata.FatafatBase.JavaRDDObject;
import com.ligadata.FatafatBase.BaseMsgObj;
import com.ligadata.FatafatBase.BaseContainerObj;

public final class CustAlertHistoryFactory {
	public static JavaRDDObject<CustAlertHistory> rddObject = CustAlertHistory$.MODULE$.toJavaRDDObject();
	public static BaseContainerObj baseObj = (BaseContainerObj) CustAlertHistory$.MODULE$;
}
