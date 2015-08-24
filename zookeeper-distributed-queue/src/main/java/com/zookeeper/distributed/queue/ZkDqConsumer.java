package com.zookeeper.distributed.queue;

import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.queue.QueueConsumer;
import com.netflix.curator.framework.state.ConnectionState;

public class ZkDqConsumer implements QueueConsumer<ZkDqWork> {

	public void stateChanged(CuratorFramework framework, ConnectionState state) {
		System.out.println("State [" + state + "]");
		
	}

	public void consumeMessage(ZkDqWork work) throws Exception {
		System.out.println("Consuming (" + work + ")");		
	}


	
	
}
