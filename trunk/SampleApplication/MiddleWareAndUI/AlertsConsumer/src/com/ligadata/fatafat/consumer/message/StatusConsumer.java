package com.ligadata.fatafat.consumer.message;

import kafka.consumer.KafkaStream;

public class StatusConsumer  extends AbstractConsumer  {

	public StatusConsumer(KafkaStream<byte[], byte[]> stream, int threadNumbe) {
		super(stream, threadNumbe);

	}

	@Override
	public void handleMessage(String message, int threadNumber) {
		// TODO Auto-generated method stub

	}

}
