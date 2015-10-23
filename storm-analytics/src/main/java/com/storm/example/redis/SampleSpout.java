package com.storm.example.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SampleSpout extends BaseRichSpout {
	
	private static final long serialVersionUID = 1L;
	private SpoutOutputCollector spoutOutputCollector;
	
	private static final Map<Integer, String> FIRSTNAMEMAP = new HashMap<Integer, String>();
	static {
		FIRSTNAMEMAP.put(0, "Kuntal");
		FIRSTNAMEMAP.put(1, "Shiva");
		FIRSTNAMEMAP.put(2, "John");
		FIRSTNAMEMAP.put(3, "Rock");
		FIRSTNAMEMAP.put(4, "Bajrang");
	}
	
	private static final Map<Integer, String> LASTNAME = new HashMap<Integer, String>();
	static {
		LASTNAME.put(0, "Ganesha");
		LASTNAME.put(1, "Watson");
		LASTNAME.put(2, "Ponting");
		LASTNAME.put(3, "Dravid");
		LASTNAME.put(4, "Sachin");
	}
	
	private static final Map<Integer, String> COMPANYNAME = new HashMap<Integer, String>();
	static {
		COMPANYNAME.put(0, "abc");
		COMPANYNAME.put(1, "dfg");
		COMPANYNAME.put(2, "pqr");
		COMPANYNAME.put(3, "ecd");
		COMPANYNAME.put(4, "awe");
	}

	public void open(Map conf, TopologyContext context,	SpoutOutputCollector spoutOutputCollector) {
		// Open the spout
		this.spoutOutputCollector = spoutOutputCollector;
	}

	public void nextTuple() {
		// Storm cluster repeatedly call this method to emit the continuous //
		// stream of tuples.
		final Random rand = new Random();
		// generate the random number from 0 to 4.
		int randomNumber = rand.nextInt(5);
		spoutOutputCollector.emit (new Values(FIRSTNAMEMAP.get(randomNumber),LASTNAME.get(randomNumber),COMPANYNAME.get(randomNumber)));
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// emit the field site.
		declarer.declare(new Fields("firstName","lastName","companyName"));
	}
}
