package com.storm.example.statistics;

import java.util.Iterator;

import backtype.storm.generated.ClusterSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologySummary;

public class TopologyStatistics {

	public void printTopologyStatistics() {
		try {
		ThriftClient thriftClient = new ThriftClient();
		// Get the thrift client
		Client client = thriftClient.getClient();
		// Get the cluster info
		ClusterSummary clusterSummary = client.getClusterInfo();
		// Get the interator over TopologySummary class
		Iterator<TopologySummary> topologiesIterator = clusterSummary.get_topologies_iterator();
		while (topologiesIterator.hasNext()) {
			TopologySummary topologySummary = topologiesIterator.next();
			System.out.println("*************************************");
			System.out.println("ID of topology: " + topologySummary.get_id());
			System.out.println("Name of topology: " + topologySummary.get_name());
			System.out.println("Number of Executors: "
					+ topologySummary.get_num_executors());
			System.out.println("Number of Tasks: " + topologySummary.get_num_tasks());
			System.out.println("Number of Workers: "
					+ topologySummary.get_num_workers());
			System.out.println("Status of topology: " + topologySummary.get_status());
			System.out.println("Topology uptime in seconds: "
					+ topologySummary.get_uptime_secs());
			System.out.println("*************************************");
		}
		}catch (Exception exception) {
			throw new RuntimeException("Error occurred while fetching the topologies  information");
		}
	}
	public static void main(String[] args) {
		new TopologyStatistics().printTopologyStatistics();
	}
}
