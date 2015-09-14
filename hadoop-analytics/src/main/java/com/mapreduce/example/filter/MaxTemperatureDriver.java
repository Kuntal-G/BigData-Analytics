package com.mapreduce.example.filter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
/*This class is responsible for running map reduce job*/
public class MaxTemperatureDriver {

public static void main(String[] args) throws Exception {
	Configuration conf = new Configuration();
	 //conf.addResource(new Path("/home/kuntal/practice/BigData/hadoop-2.4.1/etc/hadoop/core-site.xml"));
	 //conf.addResource(new Path("/home/kuntal/practice/BigData/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
	 conf.set("fs.default.name", "hdfs://10.10.87.177:9000");
	 JobConf job = new JobConf(conf);
	 job.setJarByClass(MaxTemperatureDriver.class);
	 job.setJobName("Max Temperature");
	 

	 //FileSystem fileSystem = FileSystem.get(conf);
	 job.setInputFormat(TextInputFormat.class);
	 job.setOutputFormat(TextOutputFormat.class);

	/* FileInputFormat.setInputPaths(job, new Path("/user/kuntal/sample.txt"));
	 FileOutputFormat.setOutputPath(job,new Path("/user/kuntal/outputest"));*/
	 

	 FileInputFormat.setInputPaths(job, new Path("/input/wordcount"));
	 FileOutputFormat.setOutputPath(job,new Path("/output/wordcountrmt"));
	 

	 //FileInputFormat.addInputPath(job, new Path(args[0]));
	 //FileOutputFormat.setOutputPath(job,new Path(args[1]));

	 job.setMapperClass(MaxTemperatureMapper.class);
	 job.setReducerClass(MaxTemperatureReducer.class);

	 job.setOutputKeyClass(Text.class);
	 job.setOutputValueClass(IntWritable.class);
	 JobClient.runJob(job);
 
 }
}
