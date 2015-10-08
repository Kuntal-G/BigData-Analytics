package com.storm.example.misc;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.Map;

/**
 * Put tuples into a file.
 */
public class FileBolt implements IBasicBolt{
	
  private PrintWriter output;
  private String base;

  public FileBolt(String base) {
    this.base = base;
  }

  @Override
  public void prepare(Map conf, TopologyContext context) {
    String outputName = base + context.getThisComponentId() + "-" + context.getThisTaskId();
    try {
      output = new PrintWriter(new FileOutputStream(outputName));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Can't find output file for FileBolt: " + outputName, e);
    }
  }

  @Override
  public void execute(Tuple input, BasicOutputCollector collector) {
    String separator = "";
    for (Object v : input.getValues()) {
      output.print(separator);
      output.print(v);
      separator = "\t";
    }
    output.println();
  }

  @Override
  public void cleanup() {
    output.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // no outputs
    declarer.declare(new Fields());
  }

@Override
public Map<String, Object> getComponentConfiguration() {
	// TODO Auto-generated method stub
	return null;
}
}
