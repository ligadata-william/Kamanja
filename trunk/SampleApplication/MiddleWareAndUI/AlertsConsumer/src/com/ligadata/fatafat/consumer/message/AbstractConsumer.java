package com.ligadata.fatafat.consumer.message;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public abstract class AbstractConsumer implements Runnable {

	private KafkaStream<byte[], byte[]> stream;
	private int threadNumbe;

	public AbstractConsumer(KafkaStream<byte[], byte[]> stream, int threadNumbe) {

		this.stream = stream;
		this.threadNumbe = threadNumbe;
	}

	public void run() {

		ConsumerIterator<byte[], byte[]> it = stream.iterator();
		while (it.hasNext()) {

			handleMessage(new String(it.next().message()), threadNumbe);
		}
	}

	public abstract void handleMessage(String message, int threadNumber);



}
