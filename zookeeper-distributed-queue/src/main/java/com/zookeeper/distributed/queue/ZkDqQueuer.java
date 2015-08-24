package com.zookeeper.distributed.queue;

import com.netflix.curator.framework.recipes.queue.DistributedQueue;

public class ZkDqQueuer {
		
	public void queueMessages(DistributedQueue<ZkDqWork> queue) throws Exception {
		for (int i = 0; i < 10; i++) {
			ZkDqWork work = new ZkDqWork("zootestWork [" + i + "]");
			queue.put(work);
			System.out.println("Queued [" + i + "]");
		}
		Thread.sleep(5000);
		
	}
	
	
}
