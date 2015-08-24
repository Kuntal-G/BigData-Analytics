package com.zookeeper.distributed.queue;

import com.netflix.curator.ensemble.fixed.FixedEnsembleProvider;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.queue.DistributedQueue;
import com.netflix.curator.framework.recipes.queue.QueueBuilder;
import com.netflix.curator.retry.RetryOneTime;

public class ZkDqAppTest {
	
	public static void main(String[] args) throws Exception {
		//Queue set up
		DistributedQueue<ZkDqWork> queue;
		CuratorFramework client = CuratorFrameworkFactory.builder()
				.retryPolicy(new RetryOneTime(10)).namespace("ZkDqTest")
				.ensembleProvider(new FixedEnsembleProvider("localhost:2181"))
				.build();
		client.start();
		//Consume Message from Queue
		ZkDqConsumer consumer = new ZkDqConsumer();
		ZkDqSerializer serializer = new ZkDqSerializer();
		QueueBuilder<ZkDqWork> builder = QueueBuilder.builder(client,
				consumer, serializer, "/org/zq/test");
		queue = builder.buildQueue();
		queue.start();
				
		//Publish message to Queue
		ZkDqQueuer zkQ=new ZkDqQueuer();
		zkQ.queueMessages(queue);
		
	}

}
