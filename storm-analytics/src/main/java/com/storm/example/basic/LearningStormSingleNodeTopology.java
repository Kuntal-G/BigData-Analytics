package com.storm.example.basic;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class LearningStormSingleNodeTopology {
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("LearningStormSpout", new LearningStormSpout(), 2);
		builder.setBolt("LearningStormBolt", new LearningStormBolt(), 4).shuffleGrouping("LearningStormSpout");

		Config conf = new Config();
		conf.setNumWorkers(3);
		
		try {
			// args[0] is the name of submitted topology
			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}catch(AlreadyAliveException alreadyAliveException) {
			System.out.println(alreadyAliveException);
		} catch (InvalidTopologyException invalidTopologyException) {
			System.out.println(invalidTopologyException);
		}
	}
}
