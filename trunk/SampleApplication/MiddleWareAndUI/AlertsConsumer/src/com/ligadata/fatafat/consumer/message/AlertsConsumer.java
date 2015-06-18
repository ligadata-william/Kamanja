package com.ligadata.fatafat.consumer.message;

import kafka.consumer.KafkaStream;

import org.apache.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class AlertsConsumer extends AbstractConsumer {

	private Logger logger = Logger.getLogger(AlertsConsumer.class);

	public AlertsConsumer(KafkaStream<byte[], byte[]> stream, int threadNumbe) {
		super(stream, threadNumbe);
	}

	@Override
	public void handleMessage(String message, int threadNumber) {

		Aggregations agg = Aggregations.getInstance();
		try {
			String[] messageInfo = getAlertType(message);
			agg.add(messageInfo[0]);
			agg.addMessage("Alerts of type " + messageInfo[0] + " at "
					+ messageInfo[1]);

		} catch (ParseException e) {
			logger.error("Error while parsing message", e);
			return;
		}

	}

	private String[] getAlertType(String message) throws ParseException {

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject = (JSONObject) jsonParser.parse(message);

		JSONArray lang = (JSONArray) jsonObject.get("ModelsResult");
		JSONObject modelValue = (JSONObject) lang.get(0);
		String dateString = (String) modelValue.get("ExecutionTime");
		JSONArray outputArray = (JSONArray) modelValue.get("output");
		JSONObject alertObject = (JSONObject) outputArray.get(2);

		return new String[] { alertObject.get("Value").toString(), dateString };

	}

}
