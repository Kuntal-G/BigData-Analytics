package com.storm.example.statstics;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.BoltStats;
import backtype.storm.generated.ExecutorSpecificStats;
import backtype.storm.generated.ExecutorStats;
import backtype.storm.generated.ExecutorSummary;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Nimbus.Client;
import backtype.storm.generated.TopologyInfo;

public class BoltStatistics {

	private static final String DEFAULT = "default";
	private static final String ALL_TIME = ":all-time";

	public void printBoltStatistics(String topologyId) {

		try {
			ThriftClient thriftClient = new ThriftClient();
			// Get the Nimbus thrift server client
			Client client = thriftClient.getClient();
			
			// Get the information of given topology
			TopologyInfo topologyInfo = client.getTopologyInfo(topologyId);
			Iterator<ExecutorSummary> executorSummaryIterator = topologyInfo
					.get_executors_iterator();
			while (executorSummaryIterator.hasNext()) {
				// get the executor
				ExecutorSummary executorSummary = executorSummaryIterator.next();
				ExecutorStats executorStats = executorSummary.get_stats();
				if (executorStats != null) {
					ExecutorSpecificStats executorSpecificStats = executorStats
							.get_specific();
					String componentId = executorSummary.get_component_id();
					if (executorSpecificStats.is_set_bolt()) {
						BoltStats boltStats = executorSpecificStats.get_bolt();
						System.out.println("*************************************");
						System.out.println("Component ID of Bolt " + componentId);
						System.out.println("Transferred: "
								+ getAllTimeStat(
										executorStats.get_transferred(),
										ALL_TIME));
						System.out.println("Emitted: "
								+ getAllTimeStat(executorStats.get_emitted(),
										ALL_TIME));
						System.out.println("Acked: "
								+ getBoltStats(
										boltStats.get_acked(), ALL_TIME));
						System.out.println("Failed: "
								+ getBoltStats(
										boltStats.get_failed(), ALL_TIME));
						System.out.println("Executed : "
								+ getBoltStats(
										boltStats.get_executed(), ALL_TIME));
						System.out.println("*************************************");
					}
				}
			}
		} catch (Exception exception) {
			throw new RuntimeException("Error occurred while fetching the bolt information :"+exception);
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

	public static Long getBoltStats(
			Map<String, Map<GlobalStreamId, Long>> map, String statName) {
		if (map != null) {
			Long statValue = null;
			Map<GlobalStreamId, Long> tempMap = map.get(statName);
			Set<GlobalStreamId> key = tempMap.keySet();
			if (key.size() > 0) {
				Iterator<GlobalStreamId> iterator = key.iterator();
				statValue = tempMap.get(iterator.next());
			}
			return statValue;
		}
		return 0L;
	}
	
	public static void main(String[] args) {
		new BoltStatistics().printBoltStatistics("LearningStormClusterTopology-1-1393847956");
	}
	
}
