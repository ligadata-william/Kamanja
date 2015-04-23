package com.ligadata.fatafat.consumer.message;

import kafka.consumer.KafkaStream;

public class StatusConsumerController extends AbstractConsumerController {

	public StatusConsumerController(String zookeeper, String groupId,
			String topic, int threadsNum) {
		super(zookeeper, groupId, topic, threadsNum);

	}

	@Override
	public AbstractConsumer getNewConsumer(KafkaStream<byte[], byte[]> stream,
			int threadNumbe) {

		return new StatusConsumer(stream, threadNumbe);
	}

}
