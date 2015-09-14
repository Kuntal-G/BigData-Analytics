package com.mapreduce.example.join.mapside;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The SortByKeyReducer writes out all values for the given key, but throws out the key and writes a NullWritable
 * instead.
 *
 * @author kuntal
 */
public class SortByKeyReducer extends Reducer<Text, Text, NullWritable, Text> {

    private static final NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(nullKey,value);
        }
    }
}