package com.mapreduce.example.filter;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MaxTemperatureReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {


@Override
public void reduce(Text arg0, Iterator<IntWritable> arg1,OutputCollector<Text, IntWritable> arg2, Reporter arg3)throws IOException {
	int maxValue = Integer.MIN_VALUE;
	
	
	while(arg1.hasNext()){
	 maxValue = Math.max(maxValue, arg1.next().get());
	 }
	 
	 
	 arg2.collect(arg0, new IntWritable(maxValue));
}




}
