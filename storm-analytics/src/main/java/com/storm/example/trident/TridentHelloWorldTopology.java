package com.storm.example.trident;

import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

public class TridentHelloWorldTopology {

	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		if (args.length == 0) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("Count", conf, buildTopology());
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology());
		}
	}
	
	public static StormTopology buildTopology() {

		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();

		topology.newStream("spout1", spout)
				.shuffle()
				.each(new Fields("text", "Country"),new TridentUtility.TweetFilter())
				.groupBy(new Fields("Country"))
				.aggregate(new Fields("Country"), new Count(),new Fields("count"))
				.each(new Fields("count"), new TridentUtility.Print())
				.parallelismHint(2);

		return topology.build();
	}
}
