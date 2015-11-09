package com.mapreduce.example.join.reduceside;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Besides the usual driver code, we are-
 * 1. Adding side data (department lookup data in map file format - in HDFS) to the distributed cache
 * 2. Adding key-value pairs to the configuration, each key value pair being filename, source index.
 * This is used by the mapper, to tag data with sourceIndex.
 * 3. And lastly, we are associating all the various classes we created to the job.
 *
 * @author kuntal
 */
public class Driver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        /*
            Exit job if required arguments have not been provided
         */
        if (args.length != 4) {
            System.out.printf("Three parameters are required for DriverRSJ- <input dir1> <input dir2> " +
                  "<department map file path> <output dir>\n");
            return -1;
        }

        /*
            Job instantiation
         */
        Job job = new Job(getConf());
        Configuration conf = job.getConfiguration();
        job.setJarByClass(Driver.class);
        job.setJobName("ReduceSideJoin");

        /*
            Add side data to distributed cache
         */
        DistributedCache.addCacheArchive(new URI(args[2]), conf);

        /*
            Set sourceIndex for input files; sourceIndex is an attribute of the compositeKey, to drive order, and
            reference source Can be done dynamically; Hard-coded file names for simplicity
         */
        conf.setInt("employees", 1);
        conf.setInt("salaries", 2);
        conf.setInt("salaryhistory", 3);

        /*
            Configure remaining aspects of the job
         */
        FileInputFormat.setInputPaths(job, args[0] + "," + args[1]);
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        job.setMapperClass(MapperRSJ.class);
        job.setMapOutputKeyClass(CompositeKeyWritableRSJ.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(PartitionerRSJ.class);
        job.setSortComparatorClass(SortingComparatorRSJ.class);
        job.setGroupingComparatorClass(GroupingComparatorRSJ.class);

        job.setNumReduceTasks(4);
        job.setReducerClass(ReducerRSJ.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new Driver(), args);
        System.exit(exitCode);
    }
}