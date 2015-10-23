package com.storm.example.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * Reads Twitter's sample feed using the twitter4j library.
 * 
 */
@SuppressWarnings({ "rawtypes", "serial" })
public class TwitterSampleSpout extends BaseRichSpout {

	private SpoutOutputCollector collector;
    private LinkedBlockingQueue<Status> queue;
    private TwitterStream twitterStream;

	@Override
	public void open(Map conf, TopologyContext context,	SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}

			@Override
			public void onScrubGeo(long l, long l1) {
			}

            @Override
            public void onStallWarning(StallWarning stallWarning) {
            }

            @Override
			public void onException(Exception e) {
			}
		};

        TwitterStreamFactory factory = new TwitterStreamFactory();
        //TODO: Add twitter authentication credential to this factory instance
		twitterStream = factory.getInstance();
		twitterStream.addListener(listener);
		twitterStream.sample();
	}

	@Override
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(50);
        } else {
			collector.emit(new Values(ret));
		}
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
