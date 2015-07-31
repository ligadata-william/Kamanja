package com.ligadata.kamanja.consumer.listener;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedValueListener;
import org.apache.curator.framework.recipes.shared.SharedValueReader;
import org.apache.curator.framework.state.ConnectionState;

import com.ligadata.kamanja.consumer.Main;
import com.ligadata.kamanja.consumer.message.Aggregations;

public class StartListener implements SharedValueListener {

	@Override
	public void stateChanged(CuratorFramework arg0, ConnectionState arg1) {
	}

	@Override
	public void valueHasChanged(SharedValueReader reader, byte[] arg1)
			throws Exception {

		if (Integer.parseInt(new String(reader.getValue())) == 1) {
			Main.resetValuesOnZK();
			Aggregations.getInstance().resetCounters();
			ControlKeys.start = true;

		} else {
			ControlKeys.start = false;

		}

	}

}
