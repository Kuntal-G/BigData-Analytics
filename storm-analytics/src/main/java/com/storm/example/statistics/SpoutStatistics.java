package com.storm.example.statistics;

import java.util.Iterator;
import java.util.Map;

import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.SpoutStats;
import backtype.storm.generated.TopologyInfo;

public class SpoutStatistics {

	private static final String DEFAULT = "default";
	private static final String ALL_TIME = ":all-time";

	public void printSpoutStatistics(String topologyId) {
		try {
		ThriftClient thriftClient = new ThriftClient();
		// Get the nimbus thrift client
		Client client = thriftClient.getClient();
		// Get the information of given topology 
		TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);		
		Iterator<ExecutorSummary> executorSummaryIterator = topologyInfo
				.get_executors_iterator();
		while (executorSummaryIterator.hasNext()) {
			ExecutorSummary executorSummary = executorSummaryIterator.next();
			ExecutorStats executorStats = executorSummary.get_stats();
			if(executorStats !=null) {
			ExecutorSpecificStats executorSpecificStats = executorStats.get_specific();
			String componentId = executorSummary.get_component_id();
			// 
			if (executorSpecificStats.is_set_spout()) {
				SpoutStats spoutStats = executorSpecificStats.get_spout();
				System.out.println("*************************************");
				System.out.println("Component ID of Spout:- " + componentId);
				System.out.println("Transferred:- "
						+ getAllTimeStat(executorStats.get_transferred(),ALL_TIME));
				System.out.println("Total tuples emitted:- "
						+ getAllTimeStat(executorStats.get_emitted(), ALL_TIME));
				System.out.println("Acked: "
						+ getAllTimeStat(spoutStats.get_acked(),
								ALL_TIME));
				System.out.println("Failed: "
						+ getAllTimeStat(spoutStats.get_failed(),
								ALL_TIME));
				System.out.println("*************************************");
			}
			}
		}
		}catch (Exception exception) {
			throw new RuntimeException("Error occurred while fetching the spout information : "+exception);
		}
	}

	private static Long getAllTimeStat(Map<String, Map<String, Long>> map,
			String statName) {
		if (map != null) {
			Long statValue = null;
			Map<String, Long> tempMap = map.get(statName);
			statValue = tempMap.get(DEFAULT);
			return statValue;
		}
		return 0L;
	}
	
	public static void main(String[] args) {
		new SpoutStatistics().printSpoutStatistics("LearningStormClusterTopology-1-1393847956");
	}
}
