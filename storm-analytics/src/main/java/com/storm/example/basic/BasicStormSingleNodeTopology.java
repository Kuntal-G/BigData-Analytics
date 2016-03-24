package com.storm.example.basic;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class BasicStormSingleNodeTopology {
	public static void main(String[] args) throws AuthorizationException {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("BasicStormSpout", new BasicStormSpout(), 2);
		builder.setBolt("BasicStormBolt", new BasicStormBolt(), 4).shuffleGrouping("BasicStormSpout");

		Config conf = new Config();
		conf.setNumWorkers(3);
		
		try {
			
			StormSubmitter.submitTopology("Basic Topology", conf, builder.createTopology());
		}catch(AlreadyAliveException alreadyAliveException) {
			System.out.println(alreadyAliveException);
		} catch (InvalidTopologyException invalidTopologyException) {
			System.out.println(invalidTopologyException);
		}
	}
}
