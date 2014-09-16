package com.ligadata.rtd.consumer.message;

import kafka.consumer.KafkaStream;

public class AlertsConsumerController extends AbstractConsumerController {

	public AlertsConsumerController(String zookeeper, String groupId,
			String topic, int threadsNum) {

		super(zookeeper, groupId, topic, threadsNum);
	}

	@Override
	public AbstractConsumer getNewConsumer(KafkaStream<byte[], byte[]> stream,
			int threadNumbe) {

		return new AlertsConsumer(stream, threadNumbe);
	}

}
