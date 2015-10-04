package com.storm.example.trident;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.DRPCClient;

public class DistributedRPC {
		
	public static void main(String[] args) throws Exception {
		Config conf = new Config();
		conf.setMaxSpoutPending(20);
		LocalDRPC drpc = new LocalDRPC();
		if (args.length == 0) {
			
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("CountryCount", conf, buildTopology(drpc));
			Thread.sleep(2000);
			for(int i=0; i<100 ; i++) {
				System.out.println(drpc.execute("Count", "Japan,India,Europe"));
				Thread.sleep(1000);
				}
		} else {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
			Thread.sleep(2000);
   		  	DRPCClient client = new DRPCClient("RRPC-Server", 1234);
   		  	System.out.println(client.execute("Count", "Japan,India,Europe"));
		}
	}

	
	public static StormTopology buildTopology(LocalDRPC drpc) {

		FakeTweetSpout spout = new FakeTweetSpout(10);
		TridentTopology topology = new TridentTopology();
		TridentState countryCount = topology.newStream("faketweetspout", spout)
				.shuffle()
				.each(new Fields("text","Country"), new TridentUtility.TweetFilter()).groupBy(new Fields("Country"))
				.persistentAggregate(new MemoryMapState.Factory(),new Fields("Country"), new Count(), new Fields("count"))
				.parallelismHint(2);
		
		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
		
		topology.newDRPCStream("Count", drpc)
		.each(new Fields("args"), new TridentUtility.Split(), new Fields("Country"))					
		.stateQuery(countryCount, new Fields("Country"), new MapGet(),
				new Fields("count")).each(new Fields("count"),
				        new FilterNull());
		
		return topology.build();
	}
}
