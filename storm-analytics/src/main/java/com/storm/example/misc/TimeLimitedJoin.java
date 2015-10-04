package com.storm.example.misc;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * Does a time limited join.  This means that incoming items are retained for a limited period of
 * time or until a joining record is found.  When a join is found a composite record is emitted and
 * both incoming records are acknowledged.  When the time limit is reached, an incoming tuple that
 * is about to be discarded is acknowledged before discarding it.
 */
public class TimeLimitedJoin implements IRichBolt {
  private final long expirationTime;
  private final int maxTuplesToRetain;
  private final Fields joinKey;
  private OutputCollector collector;

  private final Queue<TimedTuple> queue = new LinkedList<TimedTuple>();
  private final Map<Key, TimedTuple> pendingByKey = Maps.newHashMap();

  public TimeLimitedJoin(long expirationTime, int maxTuplesToRetain, Fields joinKey) {
    this.expirationTime = expirationTime;
    this.maxTuplesToRetain = maxTuplesToRetain;
    this.joinKey = joinKey;
  }

  @Override
  public void prepare(Map config, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public synchronized void execute(Tuple input) {
    // expire old items first.  This avoids accidental joins with things that should
    // have been discarded which is required for stable statistics.
    long cutoff = now() - expirationTime;
    while (queue.size() > 0 && (queue.size() > maxTuplesToRetain || queue.peek().time < cutoff)) {
      TimedTuple expiringTuple = queue.poll();
      if (expiringTuple.tuple != null) {
        collector.ack(expiringTuple.tuple);
        Key key = extractJoinKey(input);
        if (pendingByKey.get(key).time < cutoff) {
          pendingByKey.remove(key);
        }
      }
    }

    final Key key = extractJoinKey(input);
    TimedTuple match = pendingByKey.get(key);
    if (match != null) {
      if (match.tuple != null) {
        pendingByKey.remove(key);
        collector.emit(Arrays.asList(input, match.tuple), ImmutableList.of(Lists.newArrayList(key), match.tuple, input));
        collector.ack(input);
        collector.ack(match.tuple);
        match.tuple = null;
      }
    } else {
      final TimedTuple t = new TimedTuple(now(), input);
      queue.add(t);
      pendingByKey.put(key, t);
    }
  }

  private Key extractJoinKey(Tuple input) {
    List<Object> keys = Lists.newArrayList();
    for (String key : joinKey) {
      keys.add(input.getValueByField(key));
    }
    return new Key(keys);
  }

  /**
   * Current absolute time in millis according to whatever clock we currently have.
   */
  private long now() {
    return System.nanoTime() / 1000000;
  }

  @Override
  public void cleanup() {
    // nothing to do
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("old", "new"));
  }

  private static class Key implements Iterable<Object>{
    final List<Object> values;

    private Key(List<Object> values) {
      this.values = values;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }

      Key other = (Key) o;

      if (values == other.values) {
        return true;
      } else if (values == null || other.values == null) {
        return values == other.values;
      } else {
        if (values.size() != other.values.size()) {
          return false;
        }
        Iterator i = other.values.iterator();
        for (Object value : values) {
          if (!i.hasNext() || !value.equals(i.next())) {
            return false;
          }
        }
        return true;
      }
    }

    @Override
    public int hashCode() {
      int hash = 0;
      for (Object value : values) {
        hash = hash ^ value.hashCode();
      }
      return hash;
    }

    @Override
    public Iterator<Object> iterator() {
      return values.iterator();
    }
  }

  private static class TimedTuple implements Comparable<TimedTuple> {
    private final long time;
    public Tuple tuple;

    public TimedTuple(long time, Tuple tuple) {
      this.time = time;
      this.tuple = tuple;
    }

    @Override
    public int compareTo(TimedTuple other) {
      long r = (time - other.time);
      if (r < 0) {
        return -1;
      } else if (r > 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

@Override
public Map<String, Object> getComponentConfiguration() {
	// TODO Auto-generated method stub
	return null;
}
}
