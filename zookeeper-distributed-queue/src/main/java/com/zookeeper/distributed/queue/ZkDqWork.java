package com.zookeeper.distributed.queue;

public class ZkDqWork {
	private String work;

	public ZkDqWork(String work){
		this.work = work;
	}
	
	public String toString(){
		return this.work;
	}
}
