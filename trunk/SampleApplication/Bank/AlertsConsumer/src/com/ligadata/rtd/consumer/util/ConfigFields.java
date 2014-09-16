package com.ligadata.rtd.consumer.util;

public interface ConfigFields {

	String ZK_CONNECTION = "zk_connection";
	String ALERTS_QUEUE_TOPIC = "alerts_queue_topic";
	String STATUS_QUEUE_TOPIC = "status_queue_topic";
	String ALERTS_QUEUE_THREADS_NUMBER = "alerts_queue_threads_number";
	String STATUS_QUEUE_THREADS_NUMBER = "status_queue_threads_number";
	String QUEUE_CONSUMER_GROUP_ID = "queue_consumer_group_id";
	String ALERTS_AGG_TASK_PERIOD_MILLIES = "alerts_agg_task_period_millies";
}
