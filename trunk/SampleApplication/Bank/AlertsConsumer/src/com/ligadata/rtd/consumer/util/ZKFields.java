package com.ligadata.rtd.consumer.util;

public interface ZKFields {

	String PARENT = "/com/ligadata/biw/mt/";
	String START = PARENT + "start";
	String MESSAGE_TIMESTAMP = PARENT + "msg_timestamp";
	String EVENTS_PROCESSED = PARENT + "events_processed";
	String TOTAL_ALERTS = PARENT + "total_alerts";
	String UTF_ALERTS = PARENT + "utf_alerts";
	String EB1_ALERTS = PARENT + "eb1_alerts";
	String EB2_ALERTS = PARENT + "eb2_alerts";
	String NOD_ALERTS = PARENT + "nod_alerts";
	String OD1_ALERTS = PARENT + "od1_alerts";
	String OD2_ALERTS = PARENT + "od2_alerts";
	String OD3_ALERTS = PARENT + "od3_alerts";
	String LB_ALERTS = PARENT + "lb_alerts";
	String LATEST_MSGS = PARENT + "latest_msgs";
}
