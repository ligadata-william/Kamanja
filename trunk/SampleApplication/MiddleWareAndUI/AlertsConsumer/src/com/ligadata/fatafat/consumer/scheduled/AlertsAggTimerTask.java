package com.ligadata.fatafat.consumer.scheduled;

import java.util.TimerTask;

import org.apache.curator.framework.CuratorFramework;

import com.ligadata.fatafat.consumer.listener.ControlKeys;
import com.ligadata.fatafat.consumer.message.Aggregations;
import com.ligadata.fatafat.consumer.util.ZKFields;
import com.ligadata.fatafat.consumer.util.ZKUtil;

public class AlertsAggTimerTask extends TimerTask {

	@Override
	public void run() {

		if (ControlKeys.start) {
			updateAllValues();
		}

	}

	private synchronized void updateAllValues() {

		CuratorFramework client = ZKUtil.getSimpleClient(null);
		Aggregations agg = Aggregations.getInstance();
		ZKUtil.setValue(client, ZKFields.EB1_ALERTS, agg.getEB1Alerts() + "");
		ZKUtil.setValue(client, ZKFields.EB2_ALERTS, agg.getEB2Alerts() + "");
		ZKUtil.setValue(client, ZKFields.EVENTS_PROCESSED,
				agg.getEventsProcessed() + "");
		ZKUtil.setValue(client, ZKFields.LATEST_MSGS, agg.getLatestMessages());
		ZKUtil.setValue(client, ZKFields.LB_ALERTS, agg.getLBAlerts() + "");
		ZKUtil.setValue(client, ZKFields.NOD_ALERTS, agg.getNODAlerts() + "");
		ZKUtil.setValue(client, ZKFields.OD1_ALERTS, agg.getOD1Alerts() + "");
		ZKUtil.setValue(client, ZKFields.OD2_ALERTS, agg.getOD2Alerts() + "");
		ZKUtil.setValue(client, ZKFields.OD3_ALERTS, agg.getOD3Alerts() + "");
		ZKUtil.setValue(client, ZKFields.TOTAL_ALERTS, agg.getTotalAlerts()
				+ "");
		ZKUtil.setValue(client, ZKFields.UTF_ALERTS, agg.getUTFAlerts() + "");

	}
}
