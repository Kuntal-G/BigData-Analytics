package com.storm.example.basic;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class BasicStormTopology {
	public static void main(String[] args) throws AlreadyAliveException,
			InvalidTopologyException {
		TopologyBuilder builder = new TopologyBuilder();
		// set the spout class
		builder.setSpout("LearningStormSpout", new BasicStormSpout(), 2);
		// set the bolt class
		builder.setBolt("LearningStormBolt", new BasicStormBolt(), 4).shuffleGrouping("LearningStormSpout");

		Config conf = new Config();
		conf.setDebug(true);
		// create an instance of LocalCluster class for
		// executing topology in local mode.
		LocalCluster cluster = new LocalCluster();

		// LearningStormTopolgy is the name of submitted topology.
		cluster.submitTopology("LearningStormToplogy", conf,
				builder.createTopology());
		try {
			Thread.sleep(10000);
		} catch (Exception exception) {
			System.out.println("Thread interrupted exception : " + exception);
		}
		// kill the LearningStormTopology
		cluster.killTopology("LearningStormToplogy");
		// shutdown the storm test cluster
		cluster.shutdown();

	}
}
