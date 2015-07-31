package com.ligadata.samples.messages;

import com.ligadata.KamanjaBase.JavaRDDObject;
import com.ligadata.KamanjaBase.BaseMsgObj;
import com.ligadata.KamanjaBase.BaseContainerObj;

public final class CustAlertHistoryFactory {
	public static JavaRDDObject<CustAlertHistory> rddObject = CustAlertHistory$.MODULE$.toJavaRDDObject();
	public static BaseContainerObj baseObj = (BaseContainerObj) CustAlertHistory$.MODULE$;
}
